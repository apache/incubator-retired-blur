package com.nearinfinity.blur.lucene.index;

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
	public static final String SUPER_KEY = "_superkey_";
	public static final String SEP = ".";
	
	private Map<String,Map<String,Document>> documents = new HashMap<String, Map<String,Document>>();
	private String id;
	
	public String getId() {
		return id;
	}

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
		addExtra(docs);
		return docs;
	}

	private void addExtra(List<Document> docs) {
		if (docs == null || docs.isEmpty()) {
			return;
		}
		Document document = docs.get(0);
		document.add(new Field(PRIME_DOC,PRIME_DOC_VALUE,Store.NO,Index.NOT_ANALYZED_NO_NORMS));
	}

	private static Document addMetaData(String id, String superKey, Document doc) {
		doc.add(new Field(ID,id,Store.YES,Index.NO));
		doc.add(new Field(SUPER_KEY,superKey,Store.YES,Index.NO));
		return doc;
	}

	public SuperDocument addField(String columnFamily, String superKey, String name, String value, Store store, Index index) {
		getDocument(columnFamily, superKey, true).add(new Field(getFieldName(columnFamily,name),value,store,index));
		return this;
	}

	private String getFieldName(String columnFamily, String name) {
		return columnFamily + SEP + name;
	}

	public SuperDocument addFieldStoreAnalyzedNoNorms(String columnFamily, String superKey, String name, String value) {
		return addField(columnFamily, superKey, name, value, Store.YES, Index.ANALYZED_NO_NORMS);
	}
	
	public SuperDocument addFieldAnalyzedNoNorms(String columnFamily, String superKey, String name, String value) {
		return addField(columnFamily, superKey, name, value, Store.NO, Index.ANALYZED_NO_NORMS);
	}
	
	public SuperDocument addFieldStoreAnalyzed(String columnFamily, String superKey, String name, String value) {
		return addField(columnFamily, superKey, name, value, Store.YES, Index.ANALYZED);
	}
	
	public SuperDocument addFieldAnalyzed(String columnFamily, String superKey, String name, String value) {
		return addField(columnFamily, superKey, name, value, Store.NO, Index.ANALYZED);
	}
	
	public SuperDocument addFieldStoreNotAnalyzedNoNorms(String columnFamily, String superKey, String name, String value) {
		return addField(columnFamily, superKey, name, value, Store.YES, Index.NOT_ANALYZED_NO_NORMS);
	}
	
	public SuperDocument addFieldNotAnalyzedNoNorms(String columnFamily, String superKey, String name, String value) {
		return addField(columnFamily, superKey, name, value, Store.NO, Index.NOT_ANALYZED_NO_NORMS);
	}
	
	public SuperDocument addFieldNotStoreAnalyzed(String columnFamily, String superKey, String name, String value) {
		return addField(columnFamily, superKey, name, value, Store.YES, Index.NOT_ANALYZED);
	}
	
	public SuperDocument addFieldNotAnalyzed(String columnFamily, String superKey, String name, String value) {
		return addField(columnFamily, superKey, name, value, Store.NO, Index.NOT_ANALYZED);
	}

	public void setDocuments(String columnFamily, Map<String, Document> docs) {
		documents.put(columnFamily, docs);
	}

	public boolean isEmpty() {
		return documents.isEmpty();
	}

	@Override
	public String toString() {
		return "id:" + id + "," + documents;
	}
	
}
