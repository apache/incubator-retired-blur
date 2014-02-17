package org.apache.blur.console.model;

import java.util.Map;

public class Column {
	private String name;
	private boolean fullTextIndexed;
	private String subColumn;
	private String type;
	private Map<String, String> props;
	
	public Column(String name) {
		this.name = name;
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public boolean isFullTextIndexed() {
		return fullTextIndexed;
	}
	public void setFullTextIndexed(boolean fullTextIndexed) {
		this.fullTextIndexed = fullTextIndexed;
	}
	public String getSubColumn() {
		return subColumn;
	}
	public void setSubColumn(String subColumn) {
		this.subColumn = subColumn;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public Map<String, String> getProps() {
		return props;
	}
	public void setProps(Map<String, String> props) {
		this.props = props;
	}
}
