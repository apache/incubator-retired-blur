package com.nearinfinity.blur;

import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;

import com.nearinfinity.blur.index.SuperDocument;

public interface BlurQuery {
	
	SuperDocument getSuperDocument(String id);
	Map<String,Document> getSuperDocumentSlice(String id, String columnFamily);
	Document getDocument(String id, String columnFamily, String superKey);
	
	long searchFast(String query, String filter);
	long searchFast(String query, String filter, long minimum);
	
	List<BlurHit> searchFast(String query, String filter, long starting, int fetch);

}
