package io.asouquieres.kstream.stream.simpleprocess;

import io.asouquieres.avromodel.Invoice;
import io.asouquieres.avromodel.LegacyOrderEvent;
import io.asouquieres.avromodel.OrderEvent;
import io.asouquieres.avromodel.ProductData;
import io.asouquieres.kstream.helpers.AvroSerdes;
import io.asouquieres.kstream.stream.simpleprocess.processors.OrderProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import static io.asouquieres.kstream.stream.simpleprocess.SimpleProcessWithAvroConstants.PRODUCT_GKTABLE;

public class SimpleProcessWithAvroTopology {

    public static Topology getTopology() {

        StreamsBuilder builder = new StreamsBuilder();

        var orderStream = builder.stream(SimpleProcessWithAvroConstants.ORDER, Consumed.with(Serdes.String(), AvroSerdes.<OrderEvent>get()));

        var gktable = builder.globalTable(SimpleProcessWithAvroConstants.PRODUCT, Consumed.with(Serdes.String(), AvroSerdes.<ProductData>get()), Materialized.as(PRODUCT_GKTABLE));

        orderStream.process(OrderProcessor::new)
                .to(SimpleProcessWithAvroConstants.INVOICE, Produced.with(Serdes.String(), AvroSerdes.get()));

        return builder.build();
    }
}

