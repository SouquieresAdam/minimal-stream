package io.asouquieres.kstream;

import io.asouquieres.avromodel.LegacyOrderEvent;
import io.asouquieres.avromodel.LegacyOrderLine;
import io.asouquieres.avromodel.OrderEvent;
import io.asouquieres.kstream.helpers.AvroSerdes;
import io.asouquieres.kstream.helpers.StreamContext;
import io.asouquieres.kstream.stream.simpleavro.SimpleStreamWithAvroConstants;
import io.asouquieres.kstream.stream.simpleavro.SimpleStreamWithAvroTopology;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MapExampleTopologyTest {
    private TestInputTopic<String, LegacyOrderEvent> inputTopic;
    private TestOutputTopic<String, LegacyOrderEvent> outputTopic;


    @BeforeEach
    public void init() {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://dummy:1234");


        StreamContext.setProps(props);
        var top = SimpleStreamWithAvroTopology.getTopology();

        TopologyTestDriver testDriver = new TopologyTestDriver(top, props);


        System.out.println(top.describe().toString());

        inputTopic = testDriver.createInputTopic(SimpleStreamWithAvroConstants.SOURCE_TOPIC, Serdes.String().serializer(), AvroSerdes.<LegacyOrderEvent>get().serializer());
        outputTopic = testDriver.createOutputTopic(SimpleStreamWithAvroConstants.FILTERED_TOPIC, Serdes.String().deserializer(), AvroSerdes.<LegacyOrderEvent>get().deserializer());
    }

    @Test
    public void shouldNotFilterOrderWithLines() {


        var c1 = LegacyOrderEvent.newBuilder()
                .setOrderDate("2018-11-30T18:35:24.00Z")
                .setCustomerName("FirstCustomer")
                .setOrderIdentifier("Order1")
                .setOrderLine(List.of(LegacyOrderLine.newBuilder()
                                .setQuantity(4)
                                .setProductId("P1")
                        .build()))
                .build();

        inputTopic.pipeInput("key1", c1);


        var outputList =outputTopic.readValuesToList();

        assertEquals(1, outputList.size());
        assertEquals(c1, outputList.get(0));
    }

    @Test
    public void shouldFilterOrderWithoutLines() {


        var c1 = LegacyOrderEvent.newBuilder()
                .setOrderDate("2018-11-30T18:35:24.00Z")
                .setCustomerName("FirstCustomer")
                .setOrderIdentifier("Order1")
                .setOrderLine(List.of())
                .build();

        inputTopic.pipeInput("key1", c1);


        var outputList =outputTopic.readValuesToList();

        assertEquals(0, outputList.size());
    }
}
