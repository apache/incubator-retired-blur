package com.nearinfinity.agent;

import java.util.HashMap;

public class TableMap {
	private static HashMap<String, Integer> tables = new HashMap<String, Integer>();
	
	public static HashMap<String, Integer> get(){
		return tables;
	}
	
	public static HashMap<String, Integer> set(HashMap<String, Integer> newMap){
		return tables = newMap;
	}

}
