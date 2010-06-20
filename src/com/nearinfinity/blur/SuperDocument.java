package com.nearinfinity.blur;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;

public class SuperDocument {
	
	public static final String PRIME_DOC = "_prime_";
	public static final String PRIME_DOC_VALUE = "true";
	
	private Map<String,List<Document>> documents = new HashMap<String, List<Document>>();
	
	public class SuperSubDocument {
		String name;
		Document document;
	}
	
	private Document superDocument;


	public Document getSuperDocument() {
		return superDocument;
	}

	public void setSuperDocument(Document superDocument) {
		this.superDocument = superDocument;
	}

	public void addDocument(String superName, Document document) {
		List<Document> lst = getDocuments(superName);
		if (lst == null) {
			lst = new ArrayList<Document>();
			documents.put(superName, lst);
		}
		lst.add(document);
	}
	
	public int getDocumentSize(String superName) {
		List<Document> lst = getDocuments(superName);
		return lst == null ? -1 : lst.size();
	}
	
	public void add(Fieldable fieldable) {
		superDocument.add(fieldable);
	}
	
	public void add(Fieldable fieldable, String superName, int docIndex) {
		List<Document> list = documents.get(superName);
		if (list == null) {
			list = new ArrayList<Document>();
			documents.put(superName, list);
		}
		fillDocuments(list,docIndex);
		list.get(docIndex).add(fieldable);
	}
	
	private void fillDocuments(List<Document> list, int docIndex) {
		for (int i = list.size(); i <= docIndex; i++) {
			list.add(new Document());
		}
	}

	public Document getDocument(String superName, int docIndex) {
		List<Document> docs = getDocuments(superName);
		if (docs == null) {
			docs = new ArrayList<Document>();
			documents.put(superName, docs);
		}
		fillDocuments(docs,docIndex);
		return docs.get(docIndex);
	}
	
	public List<Document> getDocuments(String superName) {
		return documents.get(superName);
	}
	
	public Map<String,List<Document>> getDocuments() {
		return documents;
	}

	public Iterable<Document> getAllDocumentsForIndexing() {
		Collection<String> keys = new TreeSet<String>(documents.keySet());
		List<Document> docs = new ArrayList<Document>();
		for (String key : keys) {
			for (Document document : documents.get(key)) {
				docs.add(mergeWithSuperDocument(document));
			}
		}
		return docs;
	}

	private Document mergeWithSuperDocument(Document document) {
		for (Fieldable fieldable : superDocument.getFields()) {
			document.add(fieldable);
		}
		return document;
	}

	public SuperDocument addField(String name, String value, Store store, Index index) {
		if (superDocument == null) {
			superDocument = new Document();
		}
		superDocument.add(new Field(name,value,store,index));
		return this;
	}
	
	public SuperDocument addField(String name, String value, Store store, Index index, String superName, int docIndex) {
		getDocument(superName, docIndex).add(new Field(name,value,store,index));
		return this;
	}
	
	public SuperDocument addFieldStoreAnalyzedNoNorms(String name, String value) {
		return addField(name, value, Store.YES, Index.ANALYZED_NO_NORMS);
	}
	
	public SuperDocument addFieldAnalyzedNoNorms(String name, String value) {
		return addField(name, value, Store.NO, Index.ANALYZED_NO_NORMS);
	}
	
	public SuperDocument addFieldStoreAnalyzed(String name, String value) {
		return addField(name, value, Store.YES, Index.ANALYZED);
	}
	
	public SuperDocument addFieldAnalyzed(String name, String value) {
		return addField(name, value, Store.NO, Index.ANALYZED);
	}
	
	public SuperDocument addFieldStoreNotAnalyzedNoNorms(String name, String value) {
		return addField(name, value, Store.YES, Index.NOT_ANALYZED_NO_NORMS);
	}
	
	public SuperDocument addFieldNotAnalyzedNoNorms(String name, String value) {
		return addField(name, value, Store.NO, Index.NOT_ANALYZED_NO_NORMS);
	}
	
	public SuperDocument addFieldNotStoreAnalyzed(String name, String value) {
		return addField(name, value, Store.YES, Index.NOT_ANALYZED);
	}
	
	public SuperDocument addFieldNotAnalyzed(String name, String value) {
		return addField(name, value, Store.NO, Index.NOT_ANALYZED);
	}
	
	
	public SuperDocument addFieldStoreAnalyzedNoNorms(String name, String value, String superName, int docIndex) {
		return addField(name, value, Store.YES, Index.ANALYZED_NO_NORMS, superName, docIndex);
	}
	
	public SuperDocument addFieldAnalyzedNoNorms(String name, String value, String superName, int docIndex) {
		return addField(name, value, Store.NO, Index.ANALYZED_NO_NORMS, superName, docIndex);
	}
	
	public SuperDocument addFieldStoreAnalyzed(String name, String value, String superName, int docIndex) {
		return addField(name, value, Store.YES, Index.ANALYZED, superName, docIndex);
	}
	
	public SuperDocument addFieldAnalyzed(String name, String value, String superName, int docIndex) {
		return addField(name, value, Store.NO, Index.ANALYZED, superName, docIndex);
	}
	
	public SuperDocument addFieldStoreNotAnalyzedNoNorms(String name, String value, String superName, int docIndex) {
		return addField(name, value, Store.YES, Index.NOT_ANALYZED_NO_NORMS, superName, docIndex);
	}
	
	public SuperDocument addFieldNotAnalyzedNoNorms(String name, String value, String superName, int docIndex) {
		return addField(name, value, Store.NO, Index.NOT_ANALYZED_NO_NORMS, superName, docIndex);
	}
	
	public SuperDocument addFieldNotStoreAnalyzed(String name, String value, String superName, int docIndex) {
		return addField(name, value, Store.YES, Index.NOT_ANALYZED, superName, docIndex);
	}
	
	public SuperDocument addFieldNotAnalyzed(String name, String value, String superName, int docIndex) {
		return addField(name, value, Store.NO, Index.NOT_ANALYZED, superName, docIndex);
	}
	
}
