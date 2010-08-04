package com.nearinfinity.blur.server;

public class Error {
	
	private String error;
	
	private String page;

	public String getPage() {
		return page;
	}

	public Error setPage(String page) {
		this.page = page;
		return this;
	}

	public String getError() {
		return error;
	}

	public Error setError(String error) {
		this.error = error;
		return this;
	}

}
