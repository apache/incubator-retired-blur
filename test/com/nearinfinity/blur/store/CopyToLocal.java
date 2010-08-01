package com.nearinfinity.blur.store;

import java.io.File;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import com.nearinfinity.blur.lucene.store.BlurBaseDirectory;
import com.nearinfinity.blur.lucene.store.dao.cassandra.CassandraDao;

public class CopyToLocal {
	
	public static void main(String[] args) throws Exception {
		BlurBaseDirectory directory = new BlurBaseDirectory(new CassandraDao("Keyspace1", "Standard1", "testing", ConsistencyLevel.ONE, 10, "localhost", 9160));
		Directory dest = FSDirectory.open(new File("./index"));
		Directory.copy(directory, dest, false);
	}

}
