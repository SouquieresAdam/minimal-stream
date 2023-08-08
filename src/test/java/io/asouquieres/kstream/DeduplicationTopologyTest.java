package io.asouquieres.kstream;

import io.asouquieres.avromodel.Invoice;
import io.asouquieres.kstream.helpers.AvroSerdes;
import io.asouquieres.kstream.helpers.StreamContext;
import io.asouquieres.kstream.stream.deduplication.DeduplicationConstants;
import io.asouquieres.kstream.stream.deduplication.DeduplicationTopology;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DeduplicationTopologyTest {
    private TestInputTopic<String, Invoice> input;

    private TestOutputTopic<String, Invoice> output;


    private TopologyTestDriver driver = null;

    @BeforeEach
    public void init() {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://dummy:1234");

        StreamContext.setProps(props);
        var top = DeduplicationTopology.getTopology();

        driver = new TopologyTestDriver(top, props);

        System.out.println(top.describe().toString());

        input = driver.createInputTopic(DeduplicationConstants.INPUT_WITH_DUPLICATES, Serdes.String().serializer(), AvroSerdes.<Invoice>get().serializer());

        output = driver.createOutputTopic(DeduplicationConstants.OUTPUT_NO_DUPLICATES, Serdes.String().deserializer(), AvroSerdes.<Invoice>get().deserializer());
    }

    @AfterEach
    public void tearDown() {
        driver.close();
    }

    @Test
    public void shouldAllowUniqueOrderId() {

        var c1 = Invoice.newBuilder()
                .setOrderId("O1")
                .setTotalInvoiceAmount(BigDecimal.TEN)
                .setCustomerId("C1")
                .build();

        var c2 = Invoice.newBuilder(c1).setOrderId("O2").build();

        input.pipeInput("any", c1);
        input.pipeInput("any", c2);

        var outputList = output.readValuesToList();

        // Assert both values goes through
        assertEquals(2, outputList.size());
    }

    @Test
    public void shouldDeduplicateOrderId() {


        var now = Instant.now();
        var c1 = Invoice.newBuilder()
                .setOrderId("O1")
                .setTotalInvoiceAmount(BigDecimal.TEN)
                .setCustomerId("C1")
                .build();

        var c2 = Invoice.newBuilder(c1).build(); // send exactly the same object
        var c3 = Invoice.newBuilder(c1).setCustomerId("another customer").build(); // send a slightly different value with same order Id

        input.pipeInput("any", c1,now);
        input.pipeInput("any", c2,now.plus(Duration.ofSeconds(1)));
        input.pipeInput("any", c3,now.plus(Duration.ofSeconds(2)));

        var outputList = output.readValuesToList();

        // Assert both values goes through
        assertEquals(1, outputList.size());
    }


    @Test
    public void shouldNotDeduplicateAfterWindowExpiration() {

        var c1 = Invoice.newBuilder()
                .setOrderId("O1")
                .setTotalInvoiceAmount(BigDecimal.TEN)
                .setCustomerId("C1")
                .build();

        var c2 = Invoice.newBuilder(c1).build(); // send exactly the same object

        input.pipeInput("any", c1, Instant.ofEpochSecond(Instant.EPOCH.getEpochSecond()+10));
        input.pipeInput("any", c1, Instant.ofEpochSecond(Instant.EPOCH.getEpochSecond()+100));
        input.pipeInput("any", c2, Instant.ofEpochSecond(Instant.EPOCH.getEpochSecond()+10000)); // more than 2 hours later

        var outputList = output.readValuesToList();

        // Assert both values goes through
        assertEquals(2, outputList.size());
    }
}
