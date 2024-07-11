package io.asouquieres.kstream.stream.aggregation;


import io.asouquieres.avromodel.FinalAggregated51Model;
import io.asouquieres.avromodel.InternalAggregated51Model;
import io.asouquieres.avromodel.Nice51Model;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.HashMap;

import static io.asouquieres.kstream.stream.aggregation.SimpleProcessWithAvroConstants.CUSTOM_AGGREGATION_STORE;


/**
 * This class is an low level .map method equivalent
 * It allows three mains advances usages :
 * - Accessing record headers
 * - Accessing Statestores
 * -
 */
public class AggregateProcessor implements Processor<String, Nice51Model, String, FinalAggregated51Model> {

    private ProcessorContext<String, FinalAggregated51Model> context;
    private KeyValueStore<String, InternalAggregated51Model> customStore;


    @Override
    public void init(ProcessorContext<String, FinalAggregated51Model> context) {
        Processor.super.init(context);

        // Context will provide metadata & advanced PAPI features for the current record for each process method invocation
        this.context = context;

        customStore = context.getStateStore(CUSTOM_AGGREGATION_STORE);

    }

    @Override
    public void process(Record<String, Nice51Model> record) {

        var input51association = record.value();


        var currentAggregateForCustomer = customStore.get(input51association.getCustomerId());

        if(currentAggregateForCustomer == null) {
            currentAggregateForCustomer = InternalAggregated51Model.newBuilder()
                    .setCustomerId(input51association.getCustomerId())
                    // Map of 51 association unique ID & metadata
                    .setCustomer51choices(new HashMap<String,String>())
                    .build();
        }

        if(input51association.getDeleted()) {
            currentAggregateForCustomer.getCustomer51choices().remove(input51association.getUnique51associationKey());
        }else {
            currentAggregateForCustomer.getCustomer51choices().put(input51association.getUnique51associationKey(), input51association.getMetadata());
        }

        customStore.put(input51association.getCustomerId(), currentAggregateForCustomer);


        var output = new Record<>(record.key(), internalToFinal(currentAggregateForCustomer),record.timestamp(), record.headers());
        context.forward(output); // Each forward call will send a record downstream ( after the .process in the topology)
    }

    @Override
    public void close() {}




    private FinalAggregated51Model internalToFinal(InternalAggregated51Model in) {

        return FinalAggregated51Model.newBuilder()
                .setCustomerId(in.getCustomerId())
                .setMetadatas(in.getCustomer51choices().values().stream().toList())
                .build();
    }
}
