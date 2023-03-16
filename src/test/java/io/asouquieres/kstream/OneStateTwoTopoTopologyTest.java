package io.asouquieres.kstream;

import io.asouquieres.avromodel.Invoice;
import io.asouquieres.avromodel.OrderEvent;
import io.asouquieres.avromodel.OrderLine;
import io.asouquieres.avromodel.ProductData;
import io.asouquieres.kstream.helpers.AvroSerdes;
import io.asouquieres.kstream.helpers.StreamContext;
import io.asouquieres.kstream.stream.onestatetwotopo.OneStateTwoTopoConstants;
import io.asouquieres.kstream.stream.onestatetwotopo.OneStateTwoTopoTopology;
import io.asouquieres.kstream.stream.simpleprocess.SimpleProcessWithAvroConstants;
import io.asouquieres.kstream.stream.simpleprocess.SimpleProcessWithAvroTopology;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class OneStateTwoTopoTopologyTest {
    private TestInputTopic<String, String> input;

    private TestOutputTopic<String, String> outputTopic;


    @BeforeEach
    public void init() {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://dummy:1234");


        StreamContext.setProps(props);
        var top = OneStateTwoTopoTopology.getTopology();

        TopologyTestDriver testDriver = new TopologyTestDriver(top, props);


        System.out.println(top.describe().toString());

        input = testDriver.createInputTopic(OneStateTwoTopoConstants.INPUT, Serdes.String().serializer(), Serdes.String().serializer());
        outputTopic = testDriver.createOutputTopic(OneStateTwoTopoConstants.OUTPUT, Serdes.String().deserializer(), Serdes.String().deserializer());
    }

    @Test
    public void shouldNotThrow() {


    }
}
