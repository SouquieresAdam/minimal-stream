package io.asouquieres.kstream.stream.onestatetwotopo;

import io.asouquieres.kstream.helpers.PropertiesLoader;
import io.asouquieres.kstream.helpers.StreamContext;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class OneStateTwoTopoLauncherApp {

    private static final Logger logger = LogManager.getLogger(OneStateTwoTopoLauncherApp.class);

    public static void main(String[] args) {

        // Get stream configuration
        var streamsConfiguration = PropertiesLoader.fromYaml("application.yml");

        StreamContext.setProps(streamsConfiguration);

        StreamsConfig streamsConfig = new StreamsConfig(streamsConfiguration);

        //Build topology
        var stream = new KafkaStreams(OneStateTwoTopoTopology.getTopology(), streamsConfiguration);

        // Define handler in case of unmanaged exception
        stream.setUncaughtExceptionHandler((thread, e) -> {
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
