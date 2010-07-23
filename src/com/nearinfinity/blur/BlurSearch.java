package com.nearinfinity.blur;


public interface BlurSearch {
	
	SearchFastResult searchFast(String query, String filter);
	SearchFastResult searchFast(String query, String filter, long minimum);
	SearchResult search(String query, String filter, long starting, int fetch);

}
