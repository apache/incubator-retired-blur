package com.nearinfinity.blur.index;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;

public class SuperDocument {
	
	public static final String PRIME_DOC = "_prime_";
	public static final String PRIME_DOC_VALUE = "true";
	public static final String ID = "_id_";
	private static final String SUPER_KEY = "_superkey_";
	
	private Map<String,Map<String,Document>> documents = new HashMap<String, Map<String,Document>>();
	
//	public class SuperSubDocument {
//		String name;
//		Document document;
//	}
	
	private String id;
	
	public SuperDocument(String id) {
		this.id = id;
	}

	public void addDocument(String columnFamily, String superKey, Document document) {
		Map<String, Document> map = getDocuments(columnFamily);
		if (map == null) {
			map = new TreeMap<String, Document>();
			documents.put(columnFamily, map);
		}
		map.put(superKey, document);
	}
	
	public int getDocumentCount(String columnFamily) {
		Map<String, Document> map = getDocuments(columnFamily);
		return map == null ? -1 : map.size();
	}
	
	public void add(Fieldable fieldable, String columnFamily, String superKey) {
		Map<String, Document> map = getDocuments(columnFamily);
		if (map == null) {
			map = new TreeMap<String, Document>();
			documents.put(columnFamily, map);
		}
		Document document = map.get(superKey);
		if (document == null) {
			document = new Document();
			map.put(superKey, document);
		}
		document.add(fieldable);
	}

	public Document getDocument(String columnFamily, String superKey) {
		Map<String, Document> map = getDocuments(columnFamily);
		if (map == null) {
			return null;
		}
		return map.get(superKey);
	}
	
	public Document getDocument(String columnFamily, String superKey, boolean create) {
		Map<String, Document> map = getDocuments(columnFamily);
		if (map == null) {
			map = new TreeMap<String, Document>();
			documents.put(columnFamily, map);
		}
		Document document = map.get(superKey);
		if (document == null) {
			document = new Document();
			map.put(superKey, document);
		}
		return document;
	}
	
	public Map<String,Document> getDocuments(String columnFamily) {
		return documents.get(columnFamily);
	}
	
	public Map<String,Map<String,Document>> getDocuments() {
		return documents;
	}

	public Iterable<Document> getAllDocumentsForIndexing() {
		Collection<String> columnFamilies = new TreeSet<String>(documents.keySet());
		List<Document> docs = new ArrayList<Document>();
		for (String columnFamily : columnFamilies) {
			Map<String, Document> map = getDocuments(columnFamily);
			for (Entry<String, Document> entry : map.entrySet()) {
				docs.add(addMetaData(id,entry.getKey(),entry.getValue()));
			}
		}
		return docs;
	}

	private static Document addMetaData(String id, String superKey, Document doc) {
		doc.add(new Field(ID,id,Store.YES,Index.NOT_ANALYZED_NO_NORMS));
		doc.add(new Field(SUPER_KEY,superKey,Store.YES,Index.NOT_ANALYZED_NO_NORMS));
		return doc;
	}

	public SuperDocument addField(String name, String value, Store store, Index index, String columnFamily, String superKey) {
		getDocument(columnFamily, superKey, true).add(new Field(name,value,store,index));
		return this;
	}

	public SuperDocument addFieldStoreAnalyzedNoNorms(String name, String value, String columnFamily, String superKey) {
		return addField(name, value, Store.YES, Index.ANALYZED_NO_NORMS, columnFamily, superKey);
	}
	
	public SuperDocument addFieldAnalyzedNoNorms(String name, String value, String columnFamily, String superKey) {
		return addField(name, value, Store.NO, Index.ANALYZED_NO_NORMS, columnFamily, superKey);
	}
	
	public SuperDocument addFieldStoreAnalyzed(String name, String value, String columnFamily, String superKey) {
		return addField(name, value, Store.YES, Index.ANALYZED, columnFamily, superKey);
	}
	
	public SuperDocument addFieldAnalyzed(String name, String value, String columnFamily, String superKey) {
		return addField(name, value, Store.NO, Index.ANALYZED, columnFamily, superKey);
	}
	
	public SuperDocument addFieldStoreNotAnalyzedNoNorms(String name, String value, String columnFamily, String superKey) {
		return addField(name, value, Store.YES, Index.NOT_ANALYZED_NO_NORMS, columnFamily, superKey);
	}
	
	public SuperDocument addFieldNotAnalyzedNoNorms(String name, String value, String columnFamily, String superKey) {
		return addField(name, value, Store.NO, Index.NOT_ANALYZED_NO_NORMS, columnFamily, superKey);
	}
	
	public SuperDocument addFieldNotStoreAnalyzed(String name, String value, String columnFamily, String superKey) {
		return addField(name, value, Store.YES, Index.NOT_ANALYZED, columnFamily, superKey);
	}
	
	public SuperDocument addFieldNotAnalyzed(String name, String value, String columnFamily, String superKey) {
		return addField(name, value, Store.NO, Index.NOT_ANALYZED, columnFamily, superKey);
	}

	public void setDocuments(String columnFamily, Map<String, Document> docs) {
		documents.put(columnFamily, docs);
	}

	public boolean isEmpty() {
		return documents.isEmpty();
	}
	
}
