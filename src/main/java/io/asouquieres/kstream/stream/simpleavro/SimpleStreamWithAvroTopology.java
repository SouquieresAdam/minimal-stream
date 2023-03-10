package io.asouquieres.kstream.stream.simpleavro;

import io.asouquieres.avromodel.LegacyOrderEvent;
import io.asouquieres.avromodel.OrderEvent;
import io.asouquieres.kstream.helpers.AvroSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

public class SimpleStreamWithAvroTopology {

    public static Topology getTopology() {

        StreamsBuilder builder = new StreamsBuilder();

        var sourceStream = builder.stream(SimpleStreamWithAvroConstants.SOURCE_TOPIC, Consumed.with(Serdes.String(), AvroSerdes.<LegacyOrderEvent>get()));

        sourceStream.filter((k,v) -> v.getOrderLine() != null && v.getOrderLine().size() > 0)
                .to(SimpleStreamWithAvroConstants.FILTERED_TOPIC, Produced.with(Serdes.String(), AvroSerdes.get()));

        return builder.build();
    }
}
