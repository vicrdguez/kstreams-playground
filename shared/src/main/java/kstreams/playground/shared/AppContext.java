package kstreams.playground.shared;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.HostInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class AppContext {
    static private final Properties config = new Properties();
    static private KafkaProducer<String, byte[]> producer = null;
    static Logger logger = LoggerFactory.getLogger(AppContext.class.getName());
    static private KafkaStreams kafkaStreams = null;
    static private HostInfo hostInfo = null;

    public static Properties getConfig() {
        return config;
    }

    public static String getConfig(String key, String defaultValue) {
        if(config.containsKey(key)){
            return config.getProperty(key);
        }

        return defaultValue;
    }

    public static void loadConfig(String[] args) throws IOException {

        final InputStream propertiesStream;
        if (args.length > 0) {
            propertiesStream = new FileInputStream(args[0]);
        } else {
            // If no file is provided, try to load kafka.properties from the classpath
            propertiesStream = StreamApp.class.getClassLoader().getResourceAsStream("/kafka.properties");
        }

        assert propertiesStream != null;
        config.load(propertiesStream);
        propertiesStream.close();
    }

    public static void sendtToDLQ(String dlqTopic, Exception e, byte[] value) throws ExecutionException, InterruptedException {
        sendtToDLQ(dlqTopic, e, value, "unknown", -1, -1);
    }

    public static synchronized void sendtToDLQ(String dlqTopic, Exception e, byte[] value, String topic, int partition, long offset) throws ExecutionException, InterruptedException {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);

        logger.error("Sending message to the DLQ. Input topic {}, partition: {}, offset: {}.\nException: {}\nStacktrace: {}",
                topic, partition, offset, e.getMessage(), sw);

        if (producer == null) {
            logger.info("Creating DLQ Kafka Producer");
            Properties producerProperties = new Properties();
            producerProperties.putAll(config);
            producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            producer = new KafkaProducer<String, byte[]>(producerProperties);
        }


        ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(dlqTopic, e.getMessage(), value);
        record.headers().add(new RecordHeader("topic", new StringSerializer().serialize("", topic)));
        record.headers().add(new RecordHeader("partition", new IntegerSerializer().serialize("", partition)));
        record.headers().add(new RecordHeader("offset", new LongSerializer().serialize("", offset)));
        record.headers().add(new RecordHeader("exception", new StringSerializer().serialize("", e.getMessage())));
        record.headers().add(new RecordHeader("stacktrace", new StringSerializer().serialize("", sw.toString())));

        producer.send(record).get();
    }

    public static TopicManager getTopicManager() {
        Properties properties = new Properties();
        properties.putAll(config);

        return new TopicManager(properties);
    }
    public static HostInfo getCurrentHost() {
        if (hostInfo != null)
            return hostInfo;
        String applicationServer = config
            .getOrDefault(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:8080")
            .toString();

        String[] split = applicationServer.split(":");

        if (split.length < 2) {
            throw new RuntimeException("Invalid application.server configuration");
        }

        hostInfo = new HostInfo(split[0], Integer.parseInt(split[1]));
        return hostInfo;
    }

    public static void setProducer(KafkaProducer<String, byte[]> producer) {
        AppContext.producer = producer;
    }

    public static KafkaStreams getKafkaStreams() {
        return kafkaStreams;
    }

    public static void setKafkaStreams(KafkaStreams kafkaStreams) {
        AppContext.kafkaStreams = kafkaStreams;
    }

}
