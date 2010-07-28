package com.nearinfinity.blur.hbase.server;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ipc.HBaseRPCProtocolVersion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;

import com.nearinfinity.blur.hbase.BlurHits;
import com.nearinfinity.blur.hbase.SearchRPC;
import com.nearinfinity.blur.hbase.BlurHits.BlurHit;
import com.nearinfinity.blur.hbase.ipc.SearchRegionInterface;

public class SearchRegionServer extends HRegionServer implements SearchRegionInterface {

	static {
		SearchRPC.initialize();
	}

	public SearchRegionServer(HBaseConfiguration conf) throws IOException {
		super(conf);
	}

	@Override
	public long getProtocolVersion(final String protocol, final long clientVersion) throws IOException {
		if (protocol.equals(SearchRegionInterface.class.getName())) {
			return HBaseRPCProtocolVersion.versionID;
		}
		return super.getProtocolVersion(protocol, clientVersion);
	}
	
	private Random random = new Random();

	@Override
	public BlurHits search(String query, String filter, long start, int fetchCount) {
		return generate();
	}

	private BlurHits generate() {
		BlurHits blurHits = new BlurHits();
		blurHits.setTotalHits(Math.abs(random.nextLong()));
		for (int i = 0; i < 100; i++) {
			blurHits.add(new BlurHit(random.nextDouble(),UUID.randomUUID().toString(),"reason"));
		}
		return blurHits;
	}

	@Override
	public long searchFast(String query, String filter, long minimum) {
		return Math.abs(random.nextLong());
	}

}
