package com.nearinfinity.blur.manager;

import java.util.List;

import com.nearinfinity.bloomfilter.MurmurHash;

public class Partitioner {

	private static final int seed = 1;
	private List<String> shards;

	public Partitioner(List<String> shards) {
		this.shards = shards;
	}
	
	public String getShard(String id) {
		int hash = hash(id);
		return shards.get(hash % shards.size());
	}

	private int hash(String id) {
		byte[] data = id.getBytes();
		return MurmurHash.hash(seed, data, data.length);
	}

}
