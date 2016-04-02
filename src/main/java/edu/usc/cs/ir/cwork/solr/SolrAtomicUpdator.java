package edu.usc.cs.ir.cwork.solr;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.html.HtmlParser;
import org.apache.tika.sax.BodyContentHandler;
import org.json.JSONObject;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by tg on 3/23/16.
 */
public class SolrAtomicUpdator {

    public interface Transformer{
        SolrInputDocument transform(SolrDocument doc);
    }

    public static final Logger LOG = LoggerFactory.getLogger(SolrAtomicUpdator.class);

    @Option(name = "-conf", usage = "Config properties file", required = true)
    private File configFile;

    @Option(name = "-debug", usage = "Don't update, just print to console")
    private boolean debug;

    private SolrServer solrServer;
    private SolrDocIterator iterator;
    private Transformer transformer;
    private int batch;

    private void init(String[] args) throws IOException, ScriptException {

        CmdLineParser parser = new CmdLineParser(this);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.out.println(e.getMessage());
            parser.printUsage(System.out);
            System.exit(1);
        }

        Properties props = new Properties();
        try (InputStream stream = new FileInputStream(configFile)){
            props.load(stream);
        }
        String solrUrl = props.getProperty("solr.url");
        LOG.info("Solr Server {}", solrUrl);
        this.solrServer = new HttpSolrServer(solrUrl);
        String qry = props.getProperty("solr.qry");
        LOG.info("Query {}", qry);
        String fields = props.getProperty("solr.fl", "").trim();
        String [] fls = null;
        if (!fields.isEmpty()) {
            LOG.info("Fields {}", fields);
            fls = fields.split(",");
        }
        int start = Integer.parseInt(props.getProperty("solr.start", "0"));
        batch = Integer.parseInt(props.getProperty("solr.rows", "100"));
        LOG.info("start {}, rows {}", start, batch);
        String sort = props.getProperty("solr.sort", "").trim();
        LOG.info("Sort {}", sort);
        iterator = new SolrDocIterator(solrServer, qry, start, batch, sort, fls);
        if (props.containsKey("solr.limit")) {
            iterator.setLimit(Integer.parseInt(props.getProperty("solr.limit").trim()));
        }
    }


    public void update() throws IOException, SolrServerException {
        List<SolrInputDocument> buffer = new ArrayList<>(batch);
        long count = 0;
        long batches = 0;
        long skipped = 0;
        long logDelay = 2000;
        long st = System.currentTimeMillis();
        while (iterator.hasNext()){
            SolrDocument next = iterator.next();
            SolrInputDocument result = transformer.transform(next);
            if (result == null) {
                LOG.info("Skipped : {}", next);
                skipped++;
                continue;
            }
            count++;
            if (debug) {
                System.out.println(new JSONObject(result).toString(2));
            } else {
                buffer.add(result);
            }
            if (buffer.size() > batch) {
                try {
                    batches++;
                    solrServer.add(buffer);
                    buffer.clear();
                } catch (SolrServerException | IOException e) {
                    LOG.error(e.getMessage(), e);
                    LOG.error("Last Document = {}, == {}", next.get("id"), result);
                    throw e;
                }
            }

            if (System.currentTimeMillis() - st > logDelay) {
                LOG.info("Count = {}, skipped = {}, batches = {}. Last added = {}",
                        count, skipped, batches, next.get("id"));
                st = System.currentTimeMillis();
            }
        }

        if (!buffer.isEmpty()){
            solrServer.add(buffer);
        }
        LOG.info("Committing before exit");
        solrServer.commit();
        LOG.info("!Done!");
    }

    {
        transformer = new Transformer() {
            private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM");

            private HtmlParser parser = new HtmlParser();
            private ParseContext ctx = new ParseContext();

            private Map<String, String> keyMap =  new HashMap<String, String>(){{
                put("ner_weapon_name_ts_md", "weaponnames");
                put("ner_weapon_name_t_md", "weaponnames");
                put("ner_weapon_type_ts_md", "weapontypes");
                put("ner_weapon_type_t_md", "weapontypes");
            }};

            @Override
            public SolrInputDocument transform(SolrDocument doc) {
                SolrInputDocument res = new SolrInputDocument();
                String id = doc.getFieldValue("id").toString();
                res.setField("id", id);
                Set<String> updates = new HashSet<String>();
                if (doc.containsKey("contentType")
                        && doc.getFieldValue("contentType").toString().contains("ml")) {  //for xml or html
                    File content = new File(id);
                    if (content.exists()) {
                        Metadata md = new Metadata();
                        try(InputStream stream = new FileInputStream(content)){
                            parser.parse(stream, new BodyContentHandler(), md, ctx);
                            res.setField("title", new HashMap<String, Object>(){{put("title", md.get("title"));}});
                            updates.add("title");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    } else {
                        LOG.warn("File {} not found", content);
                    }
                }
                keyMap.keySet().stream()
                        .filter(doc::containsKey)
                        .forEach(key -> {
                            String newKey = keyMap.get(key);
                            res.setField(newKey, new HashMap<String, Object>() {{
                                put("set", doc.getFieldValues(key));
                                updates.add(key);
                            }});
                        });
                if (doc.containsKey("dates")) {
                    List<Date> dates = doc.getFieldValues("dates")
                            .stream().filter(date -> date instanceof Date)
                            .map(date -> (Date) date)
                            .map(dateFormat::format)
                            .distinct()
                            .map(s -> {
                                try {
                                    return dateFormat.parse(s);
                                } catch (ParseException e) {
                                    return null;
                                }})
                            .filter(d -> d != null)
                            .collect(Collectors.toList());
                    if (!dates.isEmpty()) {
                        res.setField("dates_aggr", new HashMap<String, Object>() {{
                            put("set", dates);
                        }});
                        updates.add("dates");
                    }
                }
                if (debug) {
                    LOG.info("Updates  for {} = {}", id, updates);
                }
                if (!updates.isEmpty()) {
                    return res;
                } else {
                    return null;
                }
            }
        };
    }

    public static void main(String[] args) throws Exception {
        //Example : args = "-conf conf/atomic-update.props ".split(" ");
        SolrAtomicUpdator update = new SolrAtomicUpdator();
        update.init(args);
        update.update();
        System.out.println("Done");
    }
}
