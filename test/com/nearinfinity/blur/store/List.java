package com.nearinfinity.blur.store;

import org.apache.cassandra.thrift.ConsistencyLevel;

import com.nearinfinity.blur.store.dao.cassandra.CassandraDao;

public class List {
	
	public static void main(String[] args) throws Exception {
		BlurDirectory directory = new BlurDirectory(new CassandraDao("Keyspace1", "Standard1", "testing", ConsistencyLevel.ONE, 10, "localhost", 9160));
		for (String file : directory.listAll()) {
			System.out.println(file + " " + directory.fileLength(file) + " " + directory.fileModified(file));
		}
	}

}
