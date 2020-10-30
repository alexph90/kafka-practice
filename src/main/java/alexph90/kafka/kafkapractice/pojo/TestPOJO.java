package alexph90.kafka.kafkapractice.pojo;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;

import java.util.HashMap;
import java.util.Map;

public class TestPOJO {

    private final Map<String, String> propertyMap = new HashMap<String, String>();

    @JsonAnySetter
    public void addProperty(String key, String value) {
        propertyMap.put(key, value);
    }

    @JsonAnyGetter
    public Map<String, String> getProperties() {
        return propertyMap;
    }

    @Override
    public String toString() {
        return "TestPOJO{" +
                "propertyMap=" + propertyMap +
                '}';
    }
}
