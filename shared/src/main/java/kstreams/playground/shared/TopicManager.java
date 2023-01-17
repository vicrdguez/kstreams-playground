package kstreams.playground.shared;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicManager implements AutoCloseable {
    private final Properties properties;

    TopicManager(Properties properties){
        this.properties = properties;
    }

    private final Logger logger = LoggerFactory.getLogger(TopicManager.class);
    private ExecutorService executorService = Executors.newFixedThreadPool(1);

    public void create(List<NewTopic> topics)
            throws InterruptedException, ExecutionException, TimeoutException {
        try (final AdminClient client = AdminClient.create(properties)) {
            logger.info("Creating topics");

            client.createTopics(topics).values().forEach( (topic, future) -> {
                try {
                    future.get();
                } catch (Exception ex) {
                    logger.info(ex.toString());
                }
            });

            Collection<String> topicNames = topics
                .stream()
                .map(NewTopic::name)
                .collect(Collectors.toCollection(LinkedList::new));

            logger.info("Asking cluster for topic descriptions");
            client
                .describeTopics(topicNames)
                .allTopicNames()
                .get(10, TimeUnit.SECONDS)
                .forEach((name, description) -> logger.info("Topic Description: {}", description.toString()));
        }
    }

    public void close() {
        if (executorService != null) {
            executorService.shutdownNow();
            executorService = null;
        }
    }
}
