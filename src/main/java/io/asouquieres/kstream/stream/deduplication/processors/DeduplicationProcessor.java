package io.asouquieres.kstream.stream.deduplication.processors;


import io.asouquieres.avromodel.Invoice;
import io.asouquieres.kstream.stream.deduplication.DeduplicationConstants;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.time.Instant;

/**
 * This transformer remove duplicates input record based on their OrderID field
 *
 * The purge of past deduplication keys is purged overtime automatically
 *
 * It assumes that :
 * - All data in with the same OrderId comes in the same partition
 */
public class DeduplicationProcessor implements Processor<String, Invoice, String, Invoice> {

    private ProcessorContext<String, Invoice> context;
    private WindowStore<String, String> deduplicationStore;


    @Override
    public void init(ProcessorContext<String, Invoice> context) {
        Processor.super.init(context);

        // Context will provide metadata & advanced PAPI features for the current record for each process method invocation
        this.context = context;

        deduplicationStore = context.getStateStore(DeduplicationConstants.CUSTOM_DEDUPLICATION_STORE);

    }

    @Override
    public void process(Record<String, Invoice> record) {

        var invoice = record.value();
        try(var it = deduplicationStore.fetch(invoice.getOrderId(), Instant.ofEpochMilli(record.timestamp()).minus(Duration.ofHours(2)).toEpochMilli(), Instant.ofEpochMilli(record.timestamp()).toEpochMilli())) {

            if(it.hasNext()) {
                // Do not forward
                return;
            }
        }
        // Store the key to avoid future duplicates
        deduplicationStore.put(invoice.getOrderId(), invoice.getOrderId(), record.timestamp());

        // Forward the value as-is
        var output = new Record<>(record.key(), record.value(),record.timestamp(), record.headers());
        context.forward(output); // Each forward call will send a record downstream ( after the .process in the topology)
    }

    @Override
    public void close() {}
}
