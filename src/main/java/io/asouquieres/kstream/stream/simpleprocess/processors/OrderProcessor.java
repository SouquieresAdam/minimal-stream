package io.asouquieres.kstream.stream.simpleprocess.processors;


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
public class OrderProcessor implements Processor<String, OrderEvent, String, Invoice> {

    private ProcessorContext<String, Invoice> context;
    private KeyValueStore<String, ValueAndTimestamp<ProductData>> productStore;


    @Override
    public void init(ProcessorContext<String, Invoice> context) {
        Processor.super.init(context);

        // Context will provide metadata & advanced PAPI features for the current record for each process method invocation
        this.context = context;

        productStore = context.getStateStore(PRODUCT_GKTABLE);

    }

    @Override
    public void process(Record<String, OrderEvent> record) {

        var inputOrder = record.value();

        // Lookup into the Global Table

        var totalPrice =  BigDecimal.ZERO;

        for(OrderLine line: inputOrder.getOrderLines()) {

            var productId = line.getProductId();

            var productData = productStore.get(productId);
            // Add up prices
            totalPrice = totalPrice.add(productData.value().getPrice().multiply(BigDecimal.valueOf(line.getProductQuantity())));
        }


        // Build an output Invoice
        var outputInvoice = Invoice.newBuilder()
                .setOrderId(inputOrder.getOrderId())
                .setCustomerId(inputOrder.getCustomerId())
                .setTotalInvoiceAmount(totalPrice)
                .build();

        var output = new Record<>(record.key(), outputInvoice,record.timestamp(), record.headers());

        context.forward(output); // Each forward call will send a record downstream ( after the .process in the topology)
    }

    @Override
    public void close() {}
}
