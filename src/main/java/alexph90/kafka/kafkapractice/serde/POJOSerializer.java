package alexph90.kafka.kafkapractice.serde;

import alexph90.kafka.kafkapractice.pojo.TestPOJO;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class POJOSerializer implements Serializer<TestPOJO> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    public byte[] serialize(String topic, TestPOJO testPOJO) {

        try {
            return objectMapper.writeValueAsBytes(testPOJO);
        } catch (JsonProcessingException e) {
            throw new SerializationException("Failed to serialize TestPOJO!", e);
        }
    }

}
