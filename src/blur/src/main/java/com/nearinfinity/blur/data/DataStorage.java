package com.nearinfinity.blur.data;

import java.io.InputStream;

public interface DataStorage {

	public class DataResponse {
		
		private InputStream inputStream;
		
		private String mimeType;

		public void setInputStream(InputStream inputStream) {
			this.inputStream = inputStream;
		}

		public void setMimeType(String mimeType) {
			this.mimeType = mimeType;
		}

		public InputStream getInputStream() {
			return inputStream;
		}

		public String getMimeType() {
			return mimeType;
		}
	}
	
	void save(String id, String mimeType, InputStream inputStream);

	void fetch(String id, DataResponse response);

}
