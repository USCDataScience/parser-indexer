package edu.usc.cs.ir.cwork.tika;

import com.esotericsoftware.minlog.Log;
import com.google.gson.GsonBuilder;
import com.joestelmach.natty.DateGroup;
import edu.usc.cs.ir.cwork.solr.ContentBean;
import edu.usc.cs.ir.cwork.solr.schema.FieldMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ParseSegment;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.tika.Tika;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.geo.topic.gazetteer.GeoGazetteerClient;
import org.apache.tika.parser.geo.topic.gazetteer.Location;
import org.apache.tika.parser.ner.NamedEntityParser;
import org.apache.tika.parser.ner.corenlp.CoreNLPNERecogniser;
import org.apache.tika.parser.ner.regex.RegexNERecogniser;
import org.apache.tika.sax.ToXMLContentHandler;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static edu.usc.cs.ir.cwork.solr.SolrIndexer.MD_SUFFIX;
import static edu.usc.cs.ir.cwork.solr.SolrIndexer.asSet;
import static org.apache.tika.parser.ner.NERecogniser.DATE;
import static org.apache.tika.parser.ner.NERecogniser.LOCATION;
import static org.apache.tika.parser.ner.NERecogniser.ORGANIZATION;
import static org.apache.tika.parser.ner.NERecogniser.PERSON;

/**
 * Created by tg on 10/25/15.
 */
public class Parser {

    public static final int A_DAY_MILLIS = 24 * 60 * 60 * 1000;
    public final Logger LOG = LoggerFactory.getLogger(Parser.class);
    public static final String PHASE1_CONF = "tika-config-phase1.xml";
    public static final String PHASE2_CONF = "tika-config-phase2.xml";
    public static final String DEFAULT_CONF = "tika-config.xml";
    public static final String XML_CONTENT = "tika.xml.content";

    private static final com.joestelmach.natty.Parser NATTY_PARSER =
            new com.joestelmach.natty.Parser();
    private static Parser PHASE1;
    private static Parser PHASE2;
    private static Parser INSTANCE;
    private Tika tika;
    private GeoGazetteerClient geoClient;
    private ParseUtil parseUtil;
    private FieldMapper mapper = FieldMapper.create();
    private boolean xmlContent = false;

    public Parser(InputStream configStream) {
        try {
            TikaConfig config = new TikaConfig(configStream);
            tika = new Tika(config);
            String apiUrl = System.getProperty("gazetter.rest.api", "http://localhost:8765");
            geoClient =  new GeoGazetteerClient(apiUrl);
            LOG.info("Geo API available? {}", geoClient.checkAvail());
            String nutchHome = System.getProperty("nutch.home", null);
            if (nutchHome != null) {
                LOG.info("Initializing nutch home from {}", nutchHome);
                Configuration nutchConf = NutchConfiguration.create();
                nutchConf.set("plugin.folders", new File(nutchHome, "plugins").getAbsolutePath());
                nutchConf.setInt("parser.timeout", 10);
                URLClassLoader loader = new URLClassLoader(
                        new URL[]{new File(nutchHome, "conf").toURI().toURL()},
                        nutchConf.getClassLoader());
                nutchConf.setClassLoader(loader);
                parseUtil = new ParseUtil(nutchConf);
            } else {
                LOG.warn("Nutch Home not set");
            }
            // XML Content
            xmlContent = System.getProperties().containsKey(XML_CONTENT);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized static Parser getPhase1Parser(){
        if (PHASE1 == null) {
            PHASE1 = new Parser(Parser.class.getClassLoader()
                    .getResourceAsStream(PHASE1_CONF));
        }
        return PHASE1;
    }

    public  static Parser getPhase2Parser(){
        if (PHASE2 == null) {
            synchronized (Parser.class) {
                if (PHASE2 == null) {
                    String nerImpls = CoreNLPNERecogniser.class.getName()
                            + "," + RegexNERecogniser.class.getName();
                    System.setProperty(NamedEntityParser.SYS_PROP_NER_IMPL, nerImpls);
                    PHASE2 = new Parser(Parser.class.getClassLoader()
                            .getResourceAsStream(PHASE2_CONF));
                }
            }
        }
        return PHASE2;
    }

    public static Parser getInstance(){
        if (INSTANCE == null) {
            synchronized (Parser.class) {
                if (INSTANCE == null) {
                    String nerImpls = CoreNLPNERecogniser.class.getName()
                            + "," + RegexNERecogniser.class.getName();
                    System.setProperty(NamedEntityParser.SYS_PROP_NER_IMPL, nerImpls);
                    INSTANCE = new Parser(Parser.class.getClassLoader()
                            .getResourceAsStream(DEFAULT_CONF));
                }
            }
        }
        return INSTANCE;
    }

    /**
     * Parses the content
     * @param content  the nutch content to be parsed
     * @return the text content
     */
    public String parseContent(Content content){
        Pair<String, Metadata> pair = parse(content);
        return pair != null ? pair.getKey() : null;
    }

    /**
     * Parses the text content
     * @param content  the nutch content to be parsed
     * @return the text content
     */
    public Metadata parseContent(String content){
        try (InputStream stream = new ByteArrayInputStream(
                content.getBytes(StandardCharsets.UTF_8))){
            Pair<String, Metadata> result = parse(stream);
            return result == null ? null : result.getSecond();
        } catch (IOException e) {
            LOG.warn(e.getMessage(), e);
            return null;
        }
    }

    /**
     * Parses Nutch content to read text content and metadata
     * @param content nutch content
     * @return pair of text and metadata
     */
    public Pair<String, Metadata> parse(Content content){
        ByteArrayInputStream stream = new ByteArrayInputStream(content.getContent());
        try {
            return parse(stream);
        } finally {
            IOUtils.closeQuietly(stream);
        }
    }

    public Pair<String, Metadata> parse(File file) throws IOException, TikaException {
        Metadata md = new Metadata();
        try (InputStream in = TikaInputStream.get(file.toPath(), md)){
            return new Pair<>(tika.parseToString(in), md);
        }
    }


    /**
     * Parses the stream to read text content and metadata
     * @param stream the stream
     * @return pair of text content and metadata
     */
    private Pair<String, Metadata> parse(InputStream stream) {
        Metadata metadata = new Metadata();
        try {
            String text = tika.parseToString(stream, metadata);
            return new Pair<>(text, metadata);
        } catch (IOException | TikaException e) {
            LOG.warn(e.getMessage(), e);
        }
        //something bad happened
        return null;
    }

    /**
     * Parses the URL content
     * @param url
     * @return
     * @throws IOException
     */
    public Pair<String, Metadata> parse(URL url) throws IOException, TikaException {
        Metadata metadata = new Metadata();
        try (InputStream stream = url.openStream()) {
            return new Pair<>(tika.parseToString(stream, metadata), metadata);
        }
    }


    /**
     * Filters dates that are within 24 hour time from now.
     * This is useful when date parser relate relative times to now
     * @param dates dates from which todays date time needs to be removed
     * @return filtered dates
     */
    public static Set<Date> filterDates(Set<Date> dates) {
        Set<Date> result = new HashSet<>();
        if (dates != null) {
            for (Date next : dates) {
                long freshness = Math.abs(System.currentTimeMillis() - next.getTime());
                if (freshness > A_DAY_MILLIS) {
                    result.add(next);
                }
            }
        }
        return result;
    }

    public static Set<Date> parseDates(String...values) {
        Set<Date> result = new HashSet<>();
        for (String value : values) {
            if (value == null) {
                continue;
            }
            List<DateGroup> groups = null;
            synchronized (NATTY_PARSER) {
                try {
                    groups = NATTY_PARSER.parse(value);
                } catch (Exception e) {
                    Log.debug(e.getMessage());
                }
            }
            if (groups != null) {
                for (DateGroup group : groups) {
                    List<Date> dates = group.getDates();
                    if (dates != null) {
                        result.addAll(dates);
                    }
                }
            }
        }
        return filterDates(result);
    }


    /**
     * Creates Solrj Bean from file
     *
     * @param content nutch content
     * @return Solrj Bean
     */
    public ContentBean loadMetadataBean(Content content,
                                        Function<URL, String> urlToPathMapper,
                                        ContentBean bean) throws IOException, TikaException {

        URL url = new URL(content.getBaseUrl());
        String id = urlToPathMapper.apply(url);
        bean.setId(id);
        Metadata md = new Metadata();
        try (TikaInputStream stream = TikaInputStream.get(content.getContent(), md)) {
            loadContentBean(bean, md, true, stream);
        }
        bean.setUrl(content.getUrl());
        loadOutlinks(bean, urlToPathMapper, content);
        String type = bean.getContentType();
        if (type != null){
            if (type.startsWith("text") || type.contains("ml")){
                try {
                    bean.setRawContent(new String(content.getContent(), Charset.forName("UTF-8")));
                }catch (Exception e){
                    LOG.warn(e.getMessage(), e);
                }
            }
        }
        //FIXME : Add SHA1 digest
        return bean;
    }

    public void loadOutlinks(ContentBean bean, Function<URL, String> pathFunction,
                             Content content){
        if (parseUtil == null || ParseSegment.isTruncated(content)) {
            return;
        }

        try {
            ParseResult result = parseUtil.parse(content);
            if (!result.isSuccess()) {
                return;
            }
            Parse parsed = result.get(content.getUrl());
            if (parsed != null) {
                Outlink[] outlinks = parsed.getData().getOutlinks();
                if (outlinks != null && outlinks.length > 0) {
                    Set<String> uniqOutlinks = new HashSet<>();
                    Set<String> paths = new HashSet<>();
                    for (Outlink outlink : outlinks) {
                        uniqOutlinks.add(outlink.getToUrl());
                    }
                    for (String link : uniqOutlinks) {
                        paths.add(pathFunction.apply(new URL(link)));
                    }
                    bean.setOutlinks(uniqOutlinks);
                    bean.setOutpaths(paths);
                }
            } else {
                System.err.println("This shouldn't be happening");
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * Creates Solrj Bean from file
     *
     * @param file the nutch content
     * @return Solrj Bean
     */
    public ContentBean loadMetadataBean(File file, ContentBean bean)
            throws IOException, TikaException {

        bean.setId(file.toURI().toURL().toExternalForm());
        Metadata md = new Metadata();
        try (TikaInputStream stream = TikaInputStream.get(file.toPath(), md)) {
            loadContentBean(bean, md, true, stream);
        }
        return bean;
    }

    private void loadContentBean(ContentBean bean, Metadata md,
                                 boolean keepMetaCopy,
                                 TikaInputStream stream)
            throws IOException, TikaException {
        Map<String, Object> mdFields = new HashMap<>();

        String content = "";
        if (xmlContent) {
            try {

                ToXMLContentHandler handler = new ToXMLContentHandler();
                tika.getParser().parse(stream, handler, md, new ParseContext());
                content = handler.toString();
            } catch (SAXException e) {
                e.printStackTrace();
            }
        } else {
            content = tika.parseToString(stream, md);
        }
        bean.setContent(content);
        try {
            for (String name : md.names()) {
                boolean special = false;
                if (name.startsWith("NER_")) {
                    special = true; //could be special
                    String nameType = name.substring("NER_".length());
                    if (DATE.equals(nameType)) {
                        Set<Date> dates = parseDates(md.getValues(name));
                        bean.setDates(dates);
                    } else if (PERSON.equals(nameType)) {
                        bean.setPersons(asSet(md.getValues(name)));
                    } else if (ORGANIZATION.equals(nameType)) {
                        bean.setOrganizations(asSet(md.getValues(name)));
                    } else if (LOCATION.equals(nameType)) {
                        Set<String> locations = asSet(md.getValues(name));
                        bean.setLocations(locations);
                        enrichGeoFields(locations, bean);
                    } else {
                        //no special casing this field!!
                        special = false;
                    }
                } else if ("Content-Type".equals(name)) {
                    bean.setContentType(md.get(name));
                }

                if (keepMetaCopy || !special) {
                    mdFields.put(name, md.isMultiValued(name)
                            ? md.getValues(name) : md.get(name));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        Map<String, Object> mappedMdFields = mapper.mapFields(mdFields, false);
        Map<String, Object> suffixedFields = new HashMap<>();
        mappedMdFields.forEach((k, v) -> {
            if (!k.endsWith(MD_SUFFIX)) {
                k += MD_SUFFIX;
            }
            suffixedFields.put(k, v);
        });
        bean.setMetadata(suffixedFields);
    }

    public void enrichGeoFields(Set<String> locationNames, ContentBean bean) {
        try {
            if (bean.getCities() == null) {
                bean.setCities(new HashSet<>());
            }
            if (bean.getCountries() == null) {
                bean.setCountries(new HashSet<>());
            }
            if (bean.getStates() == null) {
                bean.setStates(new HashSet<>());
            }
            if (bean.getGeoCoords() == null) {
                bean.setGeoCoords(new HashSet<>());
            }
            Map<String, List<Location>> locations = geoClient.getLocations(new ArrayList<>(locationNames));

            for (Map.Entry<String, List<Location>> e1 : locations.entrySet()) {
                for (Location l : e1.getValue()) {
                    bean.getCountries().add(l.getCountryCode());
                    bean.getGeoCoords().add(l.getLatitude() + "," + l.getLongitude());
                    if (l.getAdmin1Code() != null
                            && !l.getAdmin1Code().trim().isEmpty()
                            && !l.getAdmin1Code().equals("00")) {
                        bean.getStates().add(l.getAdmin1Code());
                    }
                    if (l.getAdmin2Code() != null
                            && !l.getAdmin2Code().trim().isEmpty()
                            && !l.getAdmin2Code().equals("00")) {
                        bean.getCities().add(l.getName());
                    }
                }
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }


    private static class CliArgs {

        @Option(name = "-conf", usage = "Tika config file. When not specified, default one will be used")
        public File confFile;

        @Option(name = "-in", usage = "Input file", required = true)
        public File inputFile;
    }

    public static void main(String[] args) throws IOException, TikaException, SAXException {
        CliArgs cliArg = new CliArgs();
        CmdLineParser cliParser = new CmdLineParser(cliArg);
        try {
            cliParser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            e.getParser().printUsage(System.err);
            System.exit(1);
        }
        ContentBean bean = new ContentBean();
        Parser parser;
        if (cliArg.confFile == null ) {
            System.out.println("Using the default conf");
            parser = getInstance();
        } else {
            System.out.println("Using conf from : " + cliArg.confFile);
            parser = new Parser(new FileInputStream(cliArg.confFile));
        }

        parser.loadMetadataBean(cliArg.inputFile, bean);
        String str = new GsonBuilder().setPrettyPrinting().create().toJson(bean);
        System.out.println(str);
    }
}
