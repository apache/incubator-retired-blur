package com.nearinfinity.blur.search;

import java.nio.ByteBuffer;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import com.nearinfinity.blur.messaging.MessageHandler;
import com.nearinfinity.blur.messaging.MessageUtil;
import com.nearinfinity.blur.messaging.SearchMessage;

public class SearchMessageHandler implements MessageHandler {
	
	private SuperSearcher searcher;
	private Directory directory = new RAMDirectory();
	
	public SearchMessageHandler(String[] args) throws Exception {
		IndexWriter writer = new IndexWriter(directory, new StandardAnalyzer(Version.LUCENE_CURRENT), MaxFieldLength.UNLIMITED);
		Document doc = new Document();
		doc.add(new Field("test","test",Store.YES,Index.ANALYZED_NO_NORMS));
		writer.addDocument(doc);
		writer.close();
		searcher = new SuperSearcher(directory);
	}

	@Override
	public byte[] handleMessage(byte[] message) {
		try {
			SearchMessage searchMessage = MessageUtil.getSearchMessage(message);
			Query luceneQuery = getQuery(searchMessage);
			Collector collector = getCollector(searchMessage);
			searcher.search(luceneQuery, collector);
			return getSearchResult(searchMessage, collector);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private byte[] getSearchResult(SearchMessage searchMessage, Collector collector) {
		TopScoreDocCollector topScoreDocCollector = (TopScoreDocCollector) collector;
		TopDocs topDocs = topScoreDocCollector.topDocs();
		return ByteBuffer.allocate(8).putLong(topDocs.totalHits).array();
	}

	private Collector getCollector(SearchMessage searchMessage) {
		return TopScoreDocCollector.create(10, true);
	}

	private Query getQuery(SearchMessage searchMessage) throws ParseException {
		return new SuperParser(Version.LUCENE_CURRENT, new StandardAnalyzer(Version.LUCENE_CURRENT)).parse(searchMessage.query);
	}

}
