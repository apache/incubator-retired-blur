package com.nearinfinity.blur;

import java.util.List;
import java.util.Set;

public class SearchResult {
	public long count;
	public Set<byte[]> respondingShards;
	public List<BlurHit> hits;
}
