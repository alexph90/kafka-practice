package alexph90.kafka.kafkapractice.main;

import alexph90.kafka.kafkapractice.config.KafkaConstants;
import alexph90.kafka.kafkapractice.pojo.TestPOJO;
import alexph90.kafka.kafkapractice.serde.POJODeserializer;
import alexph90.kafka.kafkapractice.serde.POJOSerializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MainClass {

    public static void main(String[] args) {

        List<TestPOJO> pojoList = createPojos();

        try (KafkaProducer<String, TestPOJO> producer = createProducer()) {

            producer.initTransactions();

            try {
                producer.beginTransaction();

                int keyInt = 0;

                for (TestPOJO pojo : pojoList) {
                    System.out.println("Producing record...");

                    String key = "KEY-" + ++keyInt;
                    ProducerRecord<String, TestPOJO> record = new ProducerRecord<>(KafkaConstants.TOPIC_NAME, key, pojo);

                    try {
                        RecordMetadata metadata = producer.send(record).get();
                        System.out.println("Record sent with key " + key + " to topic " + metadata.topic() + " to partition " +
                                metadata.partition() + " with offset " + metadata.offset());
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                }

                producer.commitTransaction();

            } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
                System.out.println("Unrecoverable exception!");
                e.printStackTrace();
            } catch (KafkaException e) {
                // For all other exceptions, just abort the transaction and try again.
                System.out.println("Aborting transaction!");
                e.printStackTrace();
                producer.abortTransaction();
            }

        }

        Consumer<String, TestPOJO> consumer = createConsumer();

        while (true) {
            System.out.println("Consuming record...");

            ConsumerRecords<String, TestPOJO> consumerRecords = consumer.poll(Duration.ofMillis(1000));

            if (consumerRecords.isEmpty()) {
                System.out.println("No more records consumed so breaking out of loop...");
                break;
            }

            consumerRecords.forEach(record -> {
                System.out.println("Record key: " + record.key());
                System.out.println("Record Value: " + record.value());
                System.out.println("Record partition: " + record.partition());
                System.out.println("Record offset: " + record.offset());
            });

            consumer.commitAsync();
        }

        consumer.close();
    }

    private static List<TestPOJO> createPojos() {
        TestPOJO pojo1 = new TestPOJO();
        pojo1.addProperty("COL1", "VAL1-1");
        pojo1.addProperty("COL2", "VAL2-1");
        pojo1.addProperty("COL3", "VAL3-1");

        TestPOJO pojo2 = new TestPOJO();
        pojo2.addProperty("COL1", "VAL1-2");
        pojo2.addProperty("COL2", "VAL2-2");
        pojo2.addProperty("COL3", "VAL3-2");

        TestPOJO pojo3 = new TestPOJO();
        pojo3.addProperty("COL1", "VAL1-3");
        pojo3.addProperty("COL2", "VAL2-3");
        pojo3.addProperty("COL3", "VAL3-3");

        List<TestPOJO> pojoList = new ArrayList<>();
        pojoList.add(pojo1);
        pojoList.add(pojo2);
        pojoList.add(pojo3);

        return pojoList;
    }

    private static KafkaProducer<String, TestPOJO> createProducer() {
        System.out.println("Creating Producer...");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BROKER);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, KafkaConstants.CLIENT_ID);
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-transactional-id");
        return new KafkaProducer<>(props, new StringSerializer(), new POJOSerializer());
    }

    private static Consumer<String, TestPOJO> createConsumer() {
        System.out.println("Creating Consumer...");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BROKER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConstants.GROUP_ID_CONFIG);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, POJODeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, KafkaConstants.MAX_POLL_RECORDS);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaConstants.OFFSET_RESET_EARLIEST);
        Consumer<String, TestPOJO> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(KafkaConstants.TOPIC_NAME));
        return consumer;
    }

}
