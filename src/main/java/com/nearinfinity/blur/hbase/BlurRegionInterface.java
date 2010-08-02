package com.nearinfinity.blur.hbase;

import org.apache.hadoop.hbase.ipc.HRegionInterface;


public interface BlurRegionInterface extends HRegionInterface {

	long searchFast(String query, String filter, long minimum);
	BlurHits search(String query, String filter, long start, int fetchCount);
	
}
