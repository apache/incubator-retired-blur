package com.nearinfinity.blur.manager;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.util.Version;

import com.nearinfinity.blur.lucene.index.SuperDocument;
import com.nearinfinity.blur.lucene.search.SuperParser;
import com.nearinfinity.blur.lucene.search.cache.Acl;
import com.nearinfinity.blur.server.BlurHits;
import com.nearinfinity.blur.server.BlurHits.BlurHit;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.blur.utils.ForkJoin;
import com.nearinfinity.blur.utils.ForkJoin.Merger;
import com.nearinfinity.blur.utils.ForkJoin.ParallelCall;

public class SearchExecutorImpl implements SearchExecutor, BlurConstants {

	private static final Log LOG = LogFactory.getLog(SearchExecutorImpl.class);

	private Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);
	private SearchManager searchManager;

	public SearchExecutorImpl(SearchManager searchManager) {
		this.searchManager = searchManager;
	}

	@Override
	public Set<String> getTables() {
		return searchManager.getTables();
	}

	public BlurHits search(ExecutorService executor, String table, String query, String acl, final long start,
			final int fetchCount) {
		try {
			final SuperParser parser = getParser(null);
			final Query q = parser.parse(query);
			Map<String, Searcher> searchers = searchManager.getSearchers(table);
			if (searchers == null || searchers.isEmpty()) {
				return EMTPY_HITS;
			}
			return ForkJoin.execute(executor, searchers.entrySet(),
					new ParallelCall<Entry<String, Searcher>, BlurHits>() {
						@Override
						public BlurHits call(Entry<String, Searcher> input) throws Exception {
							String shardId = input.getKey();
							final Searcher searcher = input.getValue();
							TopScoreDocCollector collector;
							if (parser.isFacetedSearch()) {
								throw new RuntimeException("not supported");
							} else {
								collector = TopScoreDocCollector.create((int) (start + fetchCount), true);
							}
							searcher.search((Query) q.clone(), collector);
							return getBlurHits(searcher, collector, shardId);
						}
					}).merge(new Merger<BlurHits>() {
				@Override
				public BlurHits merge(List<Future<BlurHits>> futures) throws Exception {
					BlurHits blurHits = null;
					for (Future<BlurHits> future : futures) {
						if (blurHits == null) {
							blurHits = future.get();
						} else {
							blurHits.merge(future.get());
						}
					}
					return blurHits.reduceHitsTo(fetchCount);
				}
			});
		} catch (Exception e) {
			LOG.error("Unknown error", e);
			throw new RuntimeException(e);
		}
	}

	protected BlurHits getBlurHits(Searcher searcher, TopScoreDocCollector collector, String shardId) throws CorruptIndexException, IOException {
		BlurHits blurHits = new BlurHits();
		int totalHits = collector.getTotalHits();
		blurHits.setTotalHits(totalHits);
		blurHits.setHits(shardId, totalHits);
		TopDocs topDocs = collector.topDocs();
		for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
			Document doc = searcher.doc(scoreDoc.doc);
			String id = doc.get(SuperDocument.ID);
			blurHits.add(new BlurHit(scoreDoc.score, id, "unknown"));
		}
		return blurHits;
	}

	public long searchFast(ExecutorService executor, String table, String query, String acl, long minimum) {
		try {
			final Query q = getParser(null).parse(query);
			Map<String, Searcher> searchers = searchManager.getSearchers(table);
			if (searchers == null || searchers.isEmpty()) {
				return 0;
			}
			return ForkJoin.execute(executor, searchers.entrySet(), new ParallelCall<Entry<String, Searcher>, Long>() {
				@Override
				public Long call(Entry<String, Searcher> input) throws Exception {
					Searcher searcher = input.getValue();
					TopDocs topDocs;
					topDocs = searcher.search((Query) q.clone(), 10);
					return (long) topDocs.totalHits;
				}
			}).merge(new Merger<Long>() {
				@Override
				public Long merge(List<Future<Long>> futures) throws Exception {
					long total = 0;
					for (Future<Long> future : futures) {
						total += future.get();
					}
					return total;
				}
			});
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private SuperParser getParser(Acl acl) {
		return new SuperParser(Version.LUCENE_CURRENT, analyzer, acl);
	}

	@Override
	public void update() {
		// do nothing
	}

}
