package io.asouquieres.kstream.stream.onestatetwotopo;

import io.asouquieres.kstream.stream.onestatetwotopo.processors.FirstProcessor;
import io.asouquieres.kstream.stream.onestatetwotopo.processors.SecondProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

public class OneStateTwoTopoTopology {

    public static Topology getTopology() {

        StreamsBuilder builder = new StreamsBuilder();


        builder.addStateStore(Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(OneStateTwoTopoConstants.COMMON_STATESTORE), Serdes.String(), Serdes.String()));

       builder.stream(OneStateTwoTopoConstants.INPUT, Consumed.with(Serdes.String(), Serdes.String()))
               .process(FirstProcessor::new, OneStateTwoTopoConstants.COMMON_STATESTORE)
               .to(OneStateTwoTopoConstants.MIDDLE, Produced.with(Serdes.String(), Serdes.String()));

       builder.stream(OneStateTwoTopoConstants.MIDDLE, Consumed.with(Serdes.String(), Serdes.String()))
                .process(SecondProcessor::new, OneStateTwoTopoConstants.COMMON_STATESTORE)
                .to(OneStateTwoTopoConstants.OUTPUT, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }
}

