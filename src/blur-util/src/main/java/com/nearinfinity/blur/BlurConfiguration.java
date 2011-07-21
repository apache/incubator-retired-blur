package com.nearinfinity.blur;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.json.repackage.JSONException;
import org.json.repackage.JSONObject;

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
        try {
            putValues(load("/blur-default.json"));
            putValues(load("/blur-site.json"));
        } catch (JSONException e) {
            throw new IOException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private void putValues(JSONObject jsonNode) throws JSONException {
        Iterator<String> fieldNames = jsonNode.keys();
        while (fieldNames.hasNext()) {
            String name = fieldNames.next();
            String value = jsonNode.getString(name);
            properties.put(name, value);
        }
    }

    private JSONObject load(String path) throws IOException, JSONException {
        InputStream inputStream = getClass().getResourceAsStream(path);
        if (inputStream == null) {
            throw new FileNotFoundException(path);
        }
        return new JSONObject(getString(inputStream));
    }

    private String getString(InputStream inputStream) throws IOException {
        StringBuilder builder = new StringBuilder();
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        while ((line = reader.readLine()) != null) {
            builder.append(line).append(' ');
        }
        return builder.toString();
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
