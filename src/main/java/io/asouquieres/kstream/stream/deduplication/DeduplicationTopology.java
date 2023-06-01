package io.asouquieres.kstream.stream.deduplication;

import io.asouquieres.avromodel.Invoice;
import io.asouquieres.kstream.helpers.AvroSerdes;
import io.asouquieres.kstream.stream.deduplication.processors.DeduplicationProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;

import static io.asouquieres.kstream.stream.deduplication.DeduplicationConstants.CUSTOM_DEDUPLICATION_STORE;
import static io.asouquieres.kstream.stream.deduplication.DeduplicationConstants.INVOICE_BY_ORDER_ID;

public class DeduplicationTopology {

    public static Topology getTopology() {

       StreamsBuilder builder = new StreamsBuilder();

       // Create a Windows Store that will retains last value for each key for a limited period of time (Purge supported automatically by underlying RocksDB store)
       builder.addStateStore(Stores.windowStoreBuilder(Stores.persistentWindowStore(CUSTOM_DEDUPLICATION_STORE, Duration.ofHours(2),Duration.ofHours(2), false), Serdes.String(), Serdes.String()));

       builder.stream(DeduplicationConstants.INPUT_WITH_DUPLICATES, Consumed.with(Serdes.String(), AvroSerdes.<Invoice>get()))
                .selectKey((k,v) -> v.getOrderId()) // Ensure Partitioning Strategy
                .repartition(Repartitioned.with(Serdes.String(), AvroSerdes.<Invoice>get()).withName(INVOICE_BY_ORDER_ID)) // Name the internal repartition topic
                .process(DeduplicationProcessor::new, CUSTOM_DEDUPLICATION_STORE) // Apply deduplication
                .to(DeduplicationConstants.OUTPUT_NO_DUPLICATES, Produced.with(Serdes.String(), AvroSerdes.get())); // Produce de-corrupted data into a topic

        return builder.build();
    }
}

