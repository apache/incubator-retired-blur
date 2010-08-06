package com.nearinfinity.blur.data;

import org.codehaus.jackson.JsonNode;

public interface DataStorageDao {

	void save(String id, JsonNode jsonNode);

	JsonNode fetch(String id);

}
