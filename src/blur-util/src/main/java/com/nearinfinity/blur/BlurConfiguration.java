package com.nearinfinity.blur;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

public class BlurConfiguration {
    
    private Map<String,String> properties = new HashMap<String,String>();
    
    public static void main(String[] args) throws IOException {
        BlurConfiguration configuration = new BlurConfiguration();
        System.out.println(configuration.get("test"));
        System.out.println(configuration.get("test2"));
        System.out.println(configuration.get("test3","def"));
    }

    public BlurConfiguration() throws IOException {
        init();
    }
    
    private void init() throws IOException {
        putValues(load("/blur-default.json"));
        putValues(load("/blur-site.json"));
    }

    private void putValues(JsonNode jsonNode) {
        Iterator<String> fieldNames = jsonNode.getFieldNames();
        while (fieldNames.hasNext()) {
            String name = fieldNames.next();
            JsonNode node = jsonNode.get(name);
            String value = node.getValueAsText();
            properties.put(name, value);
        }
    }

    private JsonNode load(String path) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        InputStream inputStream = getClass().getResourceAsStream(path);
        if (inputStream == null) {
            throw new FileNotFoundException(path);
        }
        return mapper.readTree(inputStream);
    }

    public String get(String name) {
        return properties.get(name);
    }
    
    public String get(String name, String defaultValue) {
        String value = get(name);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }
    
    public int getInt(String name, int defaultValue) {
        String value = get(name);
        if (value == null) {
            return defaultValue;
        }
        return Integer.parseInt(value);
    }
    
    public long getLong(String name, long defaultValue) {
        String value = get(name);
        if (value == null) {
            return defaultValue;
        }
        return Long.parseLong(value);
    }
    
    public short getShort(String name, short defaultValue) {
        String value = get(name);
        if (value == null) {
            return defaultValue;
        }
        return Short.parseShort(value);
    }
    
    public void set(String name, String value) {
        properties.put(name, value);
    }
    
    public void setInt(String name, int value) {
        set(name,Integer.toString(value));
    }
    
    public void setLong(String name, long value) {
        set(name,Long.toString(value));
    }
    
    public void setShort(String name, short value) {
        set(name,Short.toString(value));
    }

}
