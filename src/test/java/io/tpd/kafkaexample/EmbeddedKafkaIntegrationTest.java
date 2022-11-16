package io.tpd.kafkaexample;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import static org.junit.jupiter.api.Assertions.assertEquals;


@EnableAutoConfiguration
@SpringBootTest(classes = Config.class)
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class EmbeddedKafkaIntegrationTest {

    @Autowired
    private GeneralTopicProducer producer;

    @Autowired
    private OutputTopicConsumer consumer;

    @Value("${topic.input}")
    private String topic;

    @Test
    public void testId1()
            throws Exception {

        this.producer.send(topic, "1", new VehicleRequestData(1L, 1D, 1D));
        Thread.sleep(3000);
        assertEquals(this.consumer.getVehicleResponseData(), new VehicleResponseData(1L, 0D));

        this.producer.send(topic, "1", new VehicleRequestData(1L, 1D, 2D));
        Thread.sleep(3000);
        assertEquals(this.consumer.getVehicleResponseData(), new VehicleResponseData(1L, 1D));

        this.producer.send(topic, "1", new VehicleRequestData(1L, 2D, 2D));
        Thread.sleep(3000);
        assertEquals(this.consumer.getVehicleResponseData(), new VehicleResponseData(1L, 2D));
    }

    @Test
    public void testId2()
            throws Exception {

        this.producer.send(topic, "2", new VehicleRequestData(2L, 1D, 1D));
        Thread.sleep(3000);
        assertEquals(this.consumer.getVehicleResponseData(), new VehicleResponseData(2L, 0D));

        this.producer.send(topic, "2", new VehicleRequestData(2L, 1D, 2D));
        Thread.sleep(3000);
        assertEquals(this.consumer.getVehicleResponseData(), new VehicleResponseData(2L, 1D));

        this.producer.send(topic, "2", new VehicleRequestData(2L, 2D, 2D));
        Thread.sleep(3000);
        assertEquals(this.consumer.getVehicleResponseData(), new VehicleResponseData(2L, 2D));
    }

    @Test
    public void testId3AndId4()
            throws Exception {
        this.producer.send(topic, "3", new VehicleRequestData(3L, 1D, 1D));
        Thread.sleep(3000);
        assertEquals(this.consumer.getVehicleResponseData(), new VehicleResponseData(3L, 0D));

        this.producer.send(topic, "4", new VehicleRequestData(4L, 1D, 1D));
        Thread.sleep(3000);
        assertEquals(this.consumer.getVehicleResponseData(), new VehicleResponseData(4L, 0D));

        this.producer.send(topic, "3", new VehicleRequestData(3L, 1D, 2D));
        Thread.sleep(3000);
        assertEquals(this.consumer.getVehicleResponseData(), new VehicleResponseData(3L, 1D));

        this.producer.send(topic, "4", new VehicleRequestData(4L, 1D, 2D));
        Thread.sleep(3000);
        assertEquals(this.consumer.getVehicleResponseData(), new VehicleResponseData(4L, 1D));

        this.producer.send(topic, "3", new VehicleRequestData(3L, 2D, 2D));
        Thread.sleep(3000);
        assertEquals(this.consumer.getVehicleResponseData(), new VehicleResponseData(3L, 2D));

        this.producer.send(topic, "4", new VehicleRequestData(4L, 2D, 2D));
        Thread.sleep(3000);
        assertEquals(this.consumer.getVehicleResponseData(), new VehicleResponseData(4L, 2D));


    }
}
