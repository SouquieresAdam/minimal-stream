package io.asouquieres.kstream.helpers;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;

import java.util.HashMap;

import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE;
import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.USER_INFO_CONFIG;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class AvroSerdes {

    public static <T extends SpecificRecord> SpecificAvroSerde<T> get() {

        var serdesConfig = new HashMap<String,String>();
        serdesConfig.put(SCHEMA_REGISTRY_URL_CONFIG, StreamContext.getProps().get(SCHEMA_REGISTRY_URL_CONFIG).toString());



        var credentialSource = StreamContext.getProps().get(AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE);
        if(credentialSource != null) {
            serdesConfig.put(BASIC_AUTH_CREDENTIALS_SOURCE, credentialSource.toString());
            serdesConfig.put(USER_INFO_CONFIG, StreamContext.getProps().get(USER_INFO_CONFIG).toString());
        }

        var specificSerdes = new SpecificAvroSerde<T>();
        specificSerdes.configure(serdesConfig, false);
        return specificSerdes;
    }
}
