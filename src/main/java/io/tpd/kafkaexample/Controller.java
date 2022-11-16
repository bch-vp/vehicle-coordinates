package io.tpd.kafkaexample;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
public class Controller {

    private static final Logger logger = LoggerFactory.getLogger(Controller.class);
    private final KafkaTemplate<String, Object> template;
    private final String inputTopic;
    private final String outputTopic;

    private final Map<Long, VehicleRequestData> lastVehicleDataMap = new HashMap();
    private final Map<Long, Double> traveledDistanceMap = new HashMap();

    public Controller(final KafkaTemplate<String, Object> template,
                      @Value("${topic.input}") final String inputTopic,
                      @Value("${topic.output}") final String outputTopic) {
        this.template = template;
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
    }

    @GetMapping("/submit")
    public String submit(@RequestBody VehicleRequestData vehicleData) {
        this.template.send(inputTopic, String.valueOf(vehicleData.id()), vehicleData);
        logger.info("All messages received");
        return "Success!";
    }

    @KafkaListener(topics = "${topic.input}", clientIdPrefix = "input-1",
            containerFactory = "kafkaListenerContainerFactory")
    public void listenAsObject1(ConsumerRecord<String, VehicleRequestData> cr,
                               @Payload VehicleRequestData payload) {

        VehicleRequestData currentVehicleRequestData = cr.value();
        Double traveledDistance = calculateTraveledDistance(currentVehicleRequestData);
        traveledDistanceMap.put(currentVehicleRequestData.id(), traveledDistance);

        VehicleResponseData vehicleResponseData = new VehicleResponseData(currentVehicleRequestData.id(), traveledDistance);
        this.template.send(outputTopic, String.valueOf(currentVehicleRequestData.id()), vehicleResponseData);
    }

    @KafkaListener(topics = "${topic.input}", clientIdPrefix = "input-2",
            containerFactory = "kafkaListenerContainerFactory")
    public void listenAsObject2(ConsumerRecord<String, VehicleRequestData> cr,
                               @Payload VehicleRequestData payload) {
        VehicleRequestData currentVehicleRequestData = cr.value();
        Double traveledDistance = calculateTraveledDistance(currentVehicleRequestData);
        traveledDistanceMap.put(currentVehicleRequestData.id(), traveledDistance);

        VehicleResponseData vehicleResponseData = new VehicleResponseData(currentVehicleRequestData.id(), traveledDistance);
        this.template.send(outputTopic, String.valueOf(currentVehicleRequestData.id()), vehicleResponseData);
    }

    @KafkaListener(topics = "${topic.input}", clientIdPrefix = "input-3",
            containerFactory = "kafkaListenerContainerFactory")
    public void listenAsObject3(ConsumerRecord<String, VehicleRequestData> cr,
                               @Payload VehicleRequestData payload) {
        VehicleRequestData currentVehicleRequestData = cr.value();
        Double traveledDistance = calculateTraveledDistance(currentVehicleRequestData);
        traveledDistanceMap.put(currentVehicleRequestData.id(), traveledDistance);

        VehicleResponseData vehicleResponseData = new VehicleResponseData(currentVehicleRequestData.id(), traveledDistance);
        this.template.send(outputTopic, String.valueOf(currentVehicleRequestData.id()), vehicleResponseData);
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

    @KafkaListener(topics = "${topic.output}", clientIdPrefix = "output",
            containerFactory = "kafkaListenerContainerFactory")
    public void listenAsObject(ConsumerRecord<String, VehicleResponseData> cr,
                               @Payload VehicleResponseData payload) {

        logger.info("Vehicle id: {}, \n Distance: {} \n", cr.value().id(), cr.value());
    }
}
