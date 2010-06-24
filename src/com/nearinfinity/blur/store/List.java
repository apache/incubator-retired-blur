package com.nearinfinity.blur.store;

import org.apache.cassandra.thrift.ConsistencyLevel;

public class List {
	
	public static void main(String[] args) throws Exception {
		CassandraDirectory directory = new CassandraDirectory("Keyspace1", "Standard1", "testing",
				ConsistencyLevel.ONE, 10, "localhost", 9160);
		for (String file : directory.listAll()) {
			System.out.println(file + " " + directory.fileLength(file) + " " + directory.fileModified(file));
		}
	}

}
