package io.asouquieres.kstream.stream.referential.processors;


import io.asouquieres.avromodel.ProductData;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import static io.asouquieres.kstream.stream.simpleprocess.SimpleProcessWithAvroConstants.PRODUCT_GKTABLE;


/**
 * This class is a low level .map method equivalent
 * It allows three mains advances usages :
 * - Accessing record headers
 * - Accessing Statestores
 * -
 */
public class ProductChangeDetectionProcessor implements Processor<String, ProductData, String, ProductData> {

    private ProcessorContext<String, ProductData> context;
    private KeyValueStore<String, ProductData> productStore;


    @Override
    public void init(ProcessorContext<String, ProductData> context) {
        Processor.super.init(context);

        // Context will provide metadata & advanced PAPI features for the current record for each process method invocation
        this.context = context;

        productStore = context.getStateStore(PRODUCT_GKTABLE);

    }

    @Override
    public void process(Record<String, ProductData> record) {

     // Check if product is already known
        var storedProduct = productStore.get(record.key());
        if (!hasChanged(storedProduct, record.value())) {
            return;
        }
        productStore.put(record.key(), record.value());
        context.forward(new Record<>(record.key(), record.value(), record.timestamp(), record.headers()));
    }

    @Override
    public void close() {}


    private boolean hasChanged(ProductData storedProduct, ProductData newProduct) {
        // Implement your own logic for product significant change detection here
        return storedProduct == null;
    }
}
