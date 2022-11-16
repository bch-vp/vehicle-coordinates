package io.tpd.kafkaexample;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class GeneralTopicProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeneralTopicProducer.class);

    @Autowired
    private KafkaTemplate<String, Object> template;

    public void send(String topic, String key, Object object) {
        LOGGER.info("Sending payload='{}' to topic='{}'", object, topic);
        this.template.send(topic, key, object);
    }
}
