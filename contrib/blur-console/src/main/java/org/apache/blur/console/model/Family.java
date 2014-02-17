package org.apache.blur.console.model;

import java.util.List;

public class Family {
	private String name;
	private List<Column> columns;
	
	public Family(String name) {
		this.name = name;
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public List<Column> getColumns() {
		return columns;
	}
	public void setColumns(List<Column> columns) {
		this.columns = columns;
	}
}
