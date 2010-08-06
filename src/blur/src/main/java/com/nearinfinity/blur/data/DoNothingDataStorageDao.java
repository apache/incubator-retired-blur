package com.nearinfinity.blur.data;

import org.codehaus.jackson.JsonNode;

public class DoNothingDataStorageDao implements DataStorageDao {

	@Override
	public JsonNode fetch(String id) {
		return null;
	}

	@Override
	public void save(String id, JsonNode jsonNode) {

	}

}
