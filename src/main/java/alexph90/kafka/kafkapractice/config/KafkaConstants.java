package alexph90.kafka.kafkapractice.config;

public interface KafkaConstants {
    String BROKER = "localhost:9092";
    String CLIENT_ID = "client1";
    String TOPIC_NAME = "test-pojo";
    String GROUP_ID_CONFIG = "consumerGroup1";
    Integer MAX_NO_MESSAGE_FOUND_COUNT = 100;
    String OFFSET_RESET_LATEST = "latest";
    String OFFSET_RESET_EARLIEST = "earliest";
    Integer MAX_POLL_RECORDS = 1;
}
