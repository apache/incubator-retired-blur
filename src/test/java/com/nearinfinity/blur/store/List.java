package com.nearinfinity.blur.store;

import org.apache.cassandra.thrift.ConsistencyLevel;

import com.nearinfinity.blur.lucene.store.BlurBaseDirectory;
import com.nearinfinity.blur.lucene.store.dao.cassandra.CassandraDao;

public class List {
	
	public static void main(String[] args) throws Exception {
		BlurBaseDirectory directory = new BlurBaseDirectory(new CassandraDao("Keyspace1", "Standard1", "testing", ConsistencyLevel.ONE, 10, "localhost", 9160));
		for (String file : directory.listAll()) {
			System.out.println(file + " " + directory.fileLength(file) + " " + directory.fileModified(file));
		}
	}

}
