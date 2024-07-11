package io.asouquieres.kstream.stream.onestatetwotopo.processors;


import io.asouquieres.avromodel.Invoice;
import io.asouquieres.avromodel.OrderEvent;
import io.asouquieres.avromodel.OrderLine;
import io.asouquieres.avromodel.ProductData;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.math.BigDecimal;

import static io.asouquieres.kstream.stream.simpleprocess.SimpleProcessWithAvroConstants.PRODUCT_GKTABLE;


/**
 * This class is an low level .map method equivalent
 * It allows three mains advances usages :
 * - Accessing record headers
 * - Accessing Statestores
 * -
 */
public class SecondProcessor implements Processor<String, String, String, String> {

    private ProcessorContext<String, String> context;
    private KeyValueStore<String, String> store;


    @Override
    public void init(ProcessorContext<String, String> context) {
        Processor.super.init(context);

        // Context will provide metadata & advanced PAPI features for the current record for each process method invocation
        this.context = context;

        store = context.getStateStore(PRODUCT_GKTABLE);

    }

    @Override
    public void process(Record<String, String> record) {

        context.forward(record);
    }

    @Override
    public void close() {}
}
