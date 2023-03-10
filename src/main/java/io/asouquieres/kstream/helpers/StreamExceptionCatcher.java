package io.asouquieres.kstream.helpers;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

public class StreamExceptionCatcher {

    public static String DLQ_NAME = "dlq.topic";
    /**
     *
     */
    public static <SK, SV> KStream<SK, SV> handle(KStream<SK, MayBeException<SV>> stream) {

        var branches = stream.split()
                .branch((k, v) -> v.streamException != null, Branched.as("Error")) //0 Error output
                .defaultBranch(Branched.as("Success")); //1 Standard output

        branches.get("Error").map( (k,v) -> KeyValue.pair(k.toString(), v.streamException.toString()))
                .to(StreamContext.getProps().getProperty(DLQ_NAME), Produced.with(Serdes.String(), Serdes.String()));

        return branches.get("Error").mapValues( v -> v.streamValue);
    }
}
