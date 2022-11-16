package io.tpd.kafkaexample;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class OutputTopicConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(OutputTopicConsumer.class);

    private VehicleResponseData vehicleResponseData;

    @KafkaListener(topics = "${topic.output}", containerFactory = "kafkaListenerContainerFactory")
    public void listenAsObject(@Payload VehicleResponseData vehicleResponseData) {

        this.vehicleResponseData = vehicleResponseData;
        LOGGER.info("Vehicle id: {}, Distance: {}", vehicleResponseData.id(), vehicleResponseData.traveledDistance());
    }

    public VehicleResponseData getVehicleResponseData() {
        return vehicleResponseData;
    }
}