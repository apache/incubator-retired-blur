package com.nearinfinity.blur.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class EchoDataStorage implements DataStorage {
	
	public static class EchoId {
		private String id;

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}
	}
	
	private static final Log LOG = LogFactory.getLog(EchoDataStorage.class);
	private ObjectMapper mapper = new ObjectMapper();
	
	@Override
	public void fetch(String table, String id, DataResponse response) {
		LOG.info("Fetching [" + table +	"] [" + id + "]");
		EchoId echoId = new EchoId();
		echoId.setId(id);
		response.setMimeType("text/html");
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			mapper.writeValue(baos, echoId);
		} catch (JsonGenerationException e) {
			throw new RuntimeException(e);
		} catch (JsonMappingException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		response.setInputStream(new ByteArrayInputStream(baos.toByteArray()));
	}
	
	@Override
	public void save(String table, String id, String mimeType, InputStream inputStream) {
		
	}

	@Override
	public Iterable<String> getIdsToBeIndexed(String table, String shardId) {
		return new HashSet<String>();
	}

}
