package io.asouquieres.kstream.stream.aggregation;

import io.asouquieres.avromodel.InternalAggregated51Model;
import io.asouquieres.avromodel.Nice51Model;
import io.asouquieres.avromodel.Raw51Model;
import io.asouquieres.kstream.helpers.AvroSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.Stores;

import static io.asouquieres.kstream.stream.aggregation.SimpleProcessWithAvroConstants.CHANGE_PER_CUSTOMER_ID;
import static io.asouquieres.kstream.stream.aggregation.SimpleProcessWithAvroConstants.CUSTOM_AGGREGATION_STORE;

public class AggregatorTopology {

    public static Topology getTopology() {

        StreamsBuilder builder = new StreamsBuilder();

        builder.addStateStore(Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(CUSTOM_AGGREGATION_STORE), Serdes.String(), AvroSerdes.<InternalAggregated51Model>get()));


        var rawStream = builder.stream(SimpleProcessWithAvroConstants.RAW_51_CHANGE, Consumed.with(Serdes.String(), AvroSerdes.<Raw51Model>get()));

        rawStream.map((k,v) -> KeyValue.pair(v.getSalesforceAssociationKey(), Nice51Model.newBuilder()
                .setCustomerId(v.getCid())
                        .setDeleted(v.getDeleted())
                        .setUnique51associationKey(v.getSalesforceAssociationKey())
                        .setMetadata(v.getMetadata())
                .build())).to(SimpleProcessWithAvroConstants.NICE_51_CHANGE, Produced.with(Serdes.String(), AvroSerdes.get()));

        builder.stream(SimpleProcessWithAvroConstants.NICE_51_CHANGE, Consumed.with(Serdes.String(), AvroSerdes.<Nice51Model>get()))
                .selectKey((k,v) -> v.getCustomerId())
                .repartition(Repartitioned.with(Serdes.String(), AvroSerdes.<Nice51Model>get()).withName(CHANGE_PER_CUSTOMER_ID))
                .process(io.asouquieres.kstream.stream.aggregation.AggregateProcessor::new, CUSTOM_AGGREGATION_STORE)
                .to(SimpleProcessWithAvroConstants.AGGREGATED_51_CHANGE, Produced.with(Serdes.String(), AvroSerdes.get()));

        return builder.build();
    }
}

