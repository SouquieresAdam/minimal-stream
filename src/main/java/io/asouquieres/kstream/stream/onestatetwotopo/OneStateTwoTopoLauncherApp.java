package io.asouquieres.kstream.stream.onestatetwotopo;

import io.asouquieres.kstream.helpers.PropertiesLoader;
import io.asouquieres.kstream.helpers.StreamContext;
import io.asouquieres.kstream.stream.deduplication.DeduplicationTopology;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CountDownLatch;


public class OneStateTwoTopoLauncherApp {

    private static final Logger logger = LogManager.getLogger(OneStateTwoTopoLauncherApp.class);

    public static void main(String[] args) {
        // Get stream configuration
        var streamsConfiguration = PropertiesLoader.fromYaml("application.yml");

        StreamContext.setProps(streamsConfiguration);

        //Build topology
        try(var stream = new KafkaStreams(OneStateTwoTopoTopology.getTopology(), streamsConfiguration)) {
            final CountDownLatch latch = new CountDownLatch(1);
            // Define handler in case of unmanaged exception
            stream.setUncaughtExceptionHandler(e -> {
                logger.error("Uncaught exception occurred in Kafka Streams. Application will shutdown !", e);
                latch.countDown();
                // Consider REPLACE_THREAD if the exception is retriable
                // Consider SHUTDOWN_APPLICATION if the exception may propagate to other instances after rebalance
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });

            // Start stream execution
            stream.start();

            // Ensure your app respond gracefully to external shutdown signal
            Runtime.getRuntime().addShutdownHook(new Thread(stream::close));


            latch.await();
        } catch (Exception e) {
            logger.fatal("Error while building the topology", e);
            System.exit(1);
        }
    }
}
