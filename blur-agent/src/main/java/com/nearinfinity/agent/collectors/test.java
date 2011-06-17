package com.nearinfinity.agent.collectors;

import java.io.IOException;
import java.util.HashMap;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class test {
	public static void main(String[] args){
		ObjectMapper mapper = new ObjectMapper();
		HashMap<String, String> input = new HashMap<String, String>();
		input.put("Hello", "Goodbye");
		input.put("Hey", "Goodbye");
		input.put("Hi", "Goodbye");
		try {
			String output = mapper.writeValueAsString(input);
			System.out.println(output);
		} catch (Exception e) {
			System.out.println("Well shit I broke something");
		}
	}

}
