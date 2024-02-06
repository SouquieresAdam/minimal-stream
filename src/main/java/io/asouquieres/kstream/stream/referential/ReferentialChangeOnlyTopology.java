package io.asouquieres.kstream.stream.referential;

import io.asouquieres.avromodel.ProductData;
import io.asouquieres.kstream.helpers.AvroSerdes;
import io.asouquieres.kstream.stream.referential.processors.ProductChangeDetectionProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

public class ReferentialChangeOnlyTopology {

    public static Topology getTopology() {

        StreamsBuilder builder = new StreamsBuilder();

        var orderStream = builder.stream(ReferentialChangeOnlyConstants.PRODUCT, Consumed.with(Serdes.String(), AvroSerdes.<ProductData>get()));

        builder.addStateStore(Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(ReferentialChangeOnlyConstants.PRODUCT_KNOWN_STATE), Serdes.String(), Serdes.String()));


        orderStream.process(ProductChangeDetectionProcessor::new)
                .to(ReferentialChangeOnlyConstants.PRODUCT_CHANGED, Produced.with(Serdes.String(), AvroSerdes.get()));

        return builder.build();
    }
}

