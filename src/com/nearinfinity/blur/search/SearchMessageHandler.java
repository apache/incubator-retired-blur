package com.nearinfinity.blur.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hbase.util.Bytes;
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
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import com.nearinfinity.blur.BlurHit;
import com.nearinfinity.blur.messaging.MessageHandler;
import com.nearinfinity.blur.messaging.MessageUtil;
import com.nearinfinity.blur.messaging.SearchMessage;

public class SearchMessageHandler implements MessageHandler {
	
	private SuperSearcher searcher;
	private Directory directory = new RAMDirectory();
	private byte[] shardName;
	
	public SearchMessageHandler(String[] args) throws Exception {
		IndexWriter writer = new IndexWriter(directory, new StandardAnalyzer(Version.LUCENE_CURRENT), MaxFieldLength.UNLIMITED);
		Document doc = new Document();
		doc.add(new Field("test","test",Store.YES,Index.ANALYZED_NO_NORMS));
		writer.addDocument(doc);
		writer.close();
		shardName = Bytes.toBytes(UUID.randomUUID().toString());
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

	private byte[] getSearchResult(SearchMessage searchMessage, Collector collector) throws IOException {
		TopScoreDocCollector topScoreDocCollector = (TopScoreDocCollector) collector;
		TopDocs topDocs = topScoreDocCollector.topDocs();
		return MessageUtil.createSearchResults(topDocs.totalHits,getBlurHits(searchMessage,topDocs),shardName);
	}

	private List<BlurHit> getBlurHits(SearchMessage searchMessage, TopDocs topDocs) {
		final ScoreDoc[] scoreDocs = topDocs.scoreDocs;
		List<BlurHit> hits = new ArrayList<BlurHit>();
		for (int i = 0; i < scoreDocs.length && i < topDocs.totalHits; i++) {
			final int index = i;
			hits.add(new BlurHit() {
				@Override
				public double getScore() {
					return scoreDocs[index].score;
				}
				@Override
				public String getReason() {
					return "";
				}
				@Override
				public String getId() {
					return Integer.toString(scoreDocs[index].doc);
				}
			});
		}
		return hits;
	}

	private Collector getCollector(SearchMessage searchMessage) {
		return TopScoreDocCollector.create(10, true);
	}

	private Query getQuery(SearchMessage searchMessage) throws ParseException {
		return new SuperParser(Version.LUCENE_CURRENT, new StandardAnalyzer(Version.LUCENE_CURRENT)).parse(searchMessage.query);
	}

}
