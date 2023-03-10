package io.asouquieres.kstream;

import io.asouquieres.avromodel.*;
import io.asouquieres.kstream.helpers.AvroSerdes;
import io.asouquieres.kstream.helpers.StreamContext;
import io.asouquieres.kstream.stream.simpleavro.SimpleStreamWithAvroConstants;
import io.asouquieres.kstream.stream.simpleavro.SimpleStreamWithAvroTopology;
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

public class SimpleProcessTopologyTest {
    private TestInputTopic<String, OrderEvent> orderInput;
    private TestInputTopic<String, ProductData> productInput;

    private TestOutputTopic<String, Invoice> outputTopic;


    @BeforeEach
    public void init() {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://dummy:1234");


        StreamContext.setProps(props);
        var top = SimpleProcessWithAvroTopology.getTopology();

        TopologyTestDriver testDriver = new TopologyTestDriver(top, props);


        System.out.println(top.describe().toString());

        orderInput = testDriver.createInputTopic(SimpleProcessWithAvroConstants.ORDER, Serdes.String().serializer(), AvroSerdes.<OrderEvent>get().serializer());
        productInput = testDriver.createInputTopic(SimpleProcessWithAvroConstants.PRODUCT, Serdes.String().serializer(), AvroSerdes.<ProductData>get().serializer());

        outputTopic = testDriver.createOutputTopic(SimpleProcessWithAvroConstants.INVOICE, Serdes.String().deserializer(), AvroSerdes.<Invoice>get().deserializer());
    }

    @Test
    public void shouldCalculatePrice() {

        var c1 = OrderEvent.newBuilder()
                .setOrderDate(Instant.now())
                .setOrderId("Order1")
                .setCustomerId("Customer1")
                .setOrderLines(List.of(OrderLine.newBuilder()
                                .setProductQuantity(4)
                                .setProductId("P1")
                        .build()))
                .build();

        productInput.pipeInput("P1", ProductData.newBuilder().setProductId("P1").setPrice(BigDecimal.valueOf(10)).build());

        orderInput.pipeInput("key1", c1);



        var outputList =outputTopic.readValuesToList();

        assertEquals(1, outputList.size());
        assertEquals(Invoice.newBuilder().setCustomerId("Customer1").setOrderId("Order1").setTotalInvoiceAmount(BigDecimal.valueOf(40)).build(), outputList.get(0));
    }
}
