package com.nearinfinity.blur;


public interface BlurSearch {
	
	SearchResult searchFast(String query, String filter);
	SearchResult searchFast(String query, String filter, long minimum);
	SearchResult search(String query, String filter, long starting, int fetch);

}
