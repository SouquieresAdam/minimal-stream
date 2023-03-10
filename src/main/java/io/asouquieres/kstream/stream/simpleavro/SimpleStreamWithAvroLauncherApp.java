package io.asouquieres.kstream.stream.simpleavro;

import io.asouquieres.kstream.helpers.PropertiesLoader;
import io.asouquieres.kstream.helpers.StreamContext;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class SimpleStreamWithAvroLauncherApp {

    private static final Logger logger = LogManager.getLogger(SimpleStreamWithAvroLauncherApp.class);

    public static void main(String[] args) {

        // Get stream configuration
        var streamsConfiguration = PropertiesLoader.fromYaml("application.yml");

        StreamContext.setProps(streamsConfiguration);

        //Build topology
        var stream = new KafkaStreams(SimpleStreamWithAvroTopology.getTopology(), streamsConfiguration);


        // Define handler in case of unmanaged exception
        stream.setUncaughtExceptionHandler( (thread, e) -> {
            logger.fatal("Exception interrupted the stream", e);
            logger.fatal("Closing all threads for " + streamsConfiguration.get(StreamsConfig.APPLICATION_ID_CONFIG));
            stream.close();
        });
        // Start stream execution
        stream.start();

        // Ensure your app respond gracefully to external shutdown signal
        Runtime.getRuntime().addShutdownHook(new Thread(stream::close));
    }
}
