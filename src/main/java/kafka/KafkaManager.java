package kafka;

import database.QueueManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

public class KafkaManager {
    private static Logger logger = LoggerFactory.getLogger(KafkaManager.class);
    private static ClassLoader loader = Thread.currentThread().getContextClassLoader();

    private static Properties kafkaProperties = new Properties();

    static {
        readKafkaProperties();
    }

    private static void readKafkaProperties() {
        try(InputStream resourceStream = loader.getResourceAsStream("kafka.properties")) {
            kafkaProperties.load(resourceStream);
        } catch (IOException e) {
            logger.error("IOException is thrown while reading Kafka properties file\n" + e.getMessage());
        }
    }

    private static KafkaConsumer<String, String> getConsumer() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProperties);
        return consumer;
    }

    private static KafkaProducer<String, String> getProducer() {
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProperties);
        return producer;
    }

    public void startStreaming(String topicName) {
        QueueManager queueManager = new QueueManager();
        KafkaProducer<String, String> producer = getProducer();

        logger.info(KafkaManager.class.getName() + " is streaming to " +  topicName + " topic");

        while(true) {
            String jsonString = queueManager.dequeue();
            if (jsonString != null) {
                producer.send(new ProducerRecord<String, String>(topicName, null, jsonString));
            }
        }
    }

    public void startListening(String topicName) {
        KafkaConsumer<String, String> consumer = KafkaManager.getConsumer();
        consumer.subscribe(Arrays.asList(topicName));

        logger.info(KafkaManager.class.getName() + " is listening to " + topicName + " topic");

        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                QueueManager.enqueue(record.value());
            }
        }
    }
}
