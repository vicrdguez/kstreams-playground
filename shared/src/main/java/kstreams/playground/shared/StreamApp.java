package kstreams.playground.shared;

import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Stream;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import scala.App;

public abstract class StreamApp {
    protected static Logger logger = LoggerFactory.getLogger(StreamApp.class.getName());
    protected KafkaStreams kafkaStreams;

    protected void run() throws Exception {
        run(null);
    }

    protected void run(Properties extraProperties) throws Exception {

        if (extraProperties != null) {
            AppContext.getConfig().putAll(extraProperties);
        }
        StreamsBuilder builder = new StreamsBuilder();
        buildTopology(builder);
        Topology topology = builder.build();
        logger.info(topology.describe().toString());

        kafkaStreams = new KafkaStreams(topology, AppContext.getConfig());
        kafkaStreams.setUncaughtExceptionHandler((e) -> {
            logger.error(null, e);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
        });

        logger.warn("Streams topology \n {}", topology.describe());

        boolean doCleanUp = Boolean.parseBoolean(AppContext.getConfig("kstreams.cleanup", "true"));
        if (doCleanUp) {
            logger.warn("Executing cleanup befor starting");
            kafkaStreams.cleanUp();
        }

        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
        AppContext.setKafkaStreams(kafkaStreams);
    }


    protected void buildTopology(StreamsBuilder builder) throws ExecutionException, InterruptedException {
    }
}
