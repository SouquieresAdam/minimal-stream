package io.asouquieres.kstream.stream.repartition;

import io.asouquieres.avromodel.LegacyOrderEvent;
import io.asouquieres.kstream.helpers.AvroSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;

public class RepartitionTopology {

    public static Topology getTopology() {

        StreamsBuilder builder = new StreamsBuilder();

        var sourceStream = builder.stream(RepartitionConstants.SOURCE_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

        sourceStream.repartition(Repartitioned.with(Serdes.String(), Serdes.String()).withName("SinglePartition"))
                .to(RepartitionConstants.FILTERED_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }
}
