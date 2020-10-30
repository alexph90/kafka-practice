package alexph90.kafka.kafkapractice.serde;

import alexph90.kafka.kafkapractice.pojo.TestPOJO;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class POJODeserializer implements Deserializer<TestPOJO> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    public TestPOJO deserialize(String topic, byte[] bytes) {

        try {
            return objectMapper.readValue(bytes, TestPOJO.class);
        } catch (IOException e) {
            throw new SerializationException("Failed to deserialize TestPOJO!", e);
        }
    }

}
