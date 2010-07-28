package com.nearinfinity.blur.hbase.ipc;

import org.apache.hadoop.hbase.ipc.HRegionInterface;

import com.nearinfinity.blur.hbase.BlurHits;

public interface SearchRegionInterface extends HRegionInterface {

	long searchFast(String query, String filter, long minimum);
	BlurHits search(String query, String filter, long start, int fetchCount);
	
}
