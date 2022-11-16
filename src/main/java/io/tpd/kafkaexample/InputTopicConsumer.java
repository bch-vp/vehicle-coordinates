package io.tpd.kafkaexample;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class InputTopicConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(InputTopicConsumer.class);
    private @Value("${topic.output}") String outputTopic;
    private @Value("${topic.input}") String inputTopic;

    @Autowired
    private GeneralTopicProducer producer;

    private final Map<Long, VehicleRequestData> lastVehicleDataMap = new HashMap();
    private final Map<Long, Double> traveledDistanceMap = new HashMap();

    @KafkaListener(topics = "${topic.input}", containerFactory = "kafkaListenerContainerFactory")
    public void listenAsObject1(@Payload VehicleRequestData vehicleRequestData) {
        LOGGER.info("Consumer-1 got payload='{}' from topic='{}'", vehicleRequestData, inputTopic);
        Double traveledDistance = calculateTraveledDistance(vehicleRequestData);
        updateMaps(vehicleRequestData, traveledDistance);
        sendMessageToOutputTopic(vehicleRequestData, traveledDistance);
    }

    @KafkaListener(topics = "${topic.input}", containerFactory = "kafkaListenerContainerFactory")
    public void listenAsObject2(@Payload VehicleRequestData vehicleRequestData) {
        LOGGER.info("Consumer-2 got payload='{}' from topic='{}'", vehicleRequestData, inputTopic);
        Double traveledDistance = calculateTraveledDistance(vehicleRequestData);
        updateMaps(vehicleRequestData, traveledDistance);
        sendMessageToOutputTopic(vehicleRequestData, traveledDistance);
    }

    @KafkaListener(topics = "${topic.input}", containerFactory = "kafkaListenerContainerFactory")
    public void listenAsObject3(@Payload VehicleRequestData vehicleRequestData) {
        LOGGER.info("Consumer-3 got payload='{}' from topic='{}'", vehicleRequestData, inputTopic);
        Double traveledDistance = calculateTraveledDistance(vehicleRequestData);
        updateMaps(vehicleRequestData, traveledDistance);
        sendMessageToOutputTopic(vehicleRequestData, traveledDistance);
    }

    private Double calculateTraveledDistance(VehicleRequestData currentVehicleRequestData){

        boolean vehicleExistInSystem = traveledDistanceMap.containsKey(currentVehicleRequestData.id());
        if (!vehicleExistInSystem) {
            lastVehicleDataMap.put(currentVehicleRequestData.id(), currentVehicleRequestData);
            return 0d;
        }

        VehicleRequestData lastVehicleData = lastVehicleDataMap.get(currentVehicleRequestData.id());
        Double distance = calculateDistanceBetweenPoints(lastVehicleData.x(), lastVehicleData.y(), currentVehicleRequestData.x(), currentVehicleRequestData.y());
        Double traveledDistance = traveledDistanceMap.get(currentVehicleRequestData.id()) + distance;
        return traveledDistance;
    }

    private Double calculateDistanceBetweenPoints(
            Double x1,
            Double y1,
            Double x2,
            Double y2) {
        return Math.sqrt((y2 - y1) * (y2 - y1) + (x2 - x1) * (x2 - x1));
    }

    private void updateMaps(VehicleRequestData payload, Double traveledDistance) {
        traveledDistanceMap.put(payload.id(), traveledDistance);
        lastVehicleDataMap.put(payload.id(), payload);
    }

    private void sendMessageToOutputTopic(VehicleRequestData payload, Double traveledDistance){
        VehicleResponseData vehicleResponseData = new VehicleResponseData(payload.id(), traveledDistance);
        this.producer.send(outputTopic, String.valueOf(payload.id()), vehicleResponseData);
    }
}