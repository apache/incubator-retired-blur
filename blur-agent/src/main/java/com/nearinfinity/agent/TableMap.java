package com.nearinfinity.agent;

import java.util.HashMap;
import java.util.Map;

public class TableMap {
	private static HashMap<String, Map<String, Object>> tables = new HashMap<String, Map<String, Object>>();
	
	public static HashMap<String, Map<String, Object>> get(){
		return tables;
	}
}
