package io.asouquieres.kstream.stream.onestatetwotopo;

import io.asouquieres.avromodel.InternalAggregated51Model;
import io.asouquieres.avromodel.OrderEvent;
import io.asouquieres.avromodel.ProductData;
import io.asouquieres.kstream.helpers.AvroSerdes;
import io.asouquieres.kstream.stream.onestatetwotopo.processors.FirstProcessor;
import io.asouquieres.kstream.stream.onestatetwotopo.processors.SecondProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

import static io.asouquieres.kstream.stream.aggregation.SimpleProcessWithAvroConstants.CUSTOM_AGGREGATE_STORE;
import static io.asouquieres.kstream.stream.simpleprocess.SimpleProcessWithAvroConstants.PRODUCT_GKTABLE;

public class OneStateTwoTopoTopology {

    public static Topology getTopology() {

        StreamsBuilder builder = new StreamsBuilder();


        builder.addStateStore(Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(OneStateTwoTopoConstants.PRODUCT_GKTABLE), Serdes.String(), Serdes.String()));

       builder.stream(OneStateTwoTopoConstants.INPUT, Consumed.with(Serdes.String(), Serdes.String()))
               .process(FirstProcessor::new, OneStateTwoTopoConstants.PRODUCT_GKTABLE)
               .to(OneStateTwoTopoConstants.MIDDLE, Produced.with(Serdes.String(), Serdes.String()));

       builder.stream(OneStateTwoTopoConstants.MIDDLE, Consumed.with(Serdes.String(), Serdes.String()))
                .process(SecondProcessor::new, OneStateTwoTopoConstants.PRODUCT_GKTABLE)
                .to(OneStateTwoTopoConstants.OUTPUT, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }
}

