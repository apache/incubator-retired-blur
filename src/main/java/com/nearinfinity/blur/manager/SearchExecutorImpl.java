package com.nearinfinity.blur.manager;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Version;

import com.nearinfinity.blur.hbase.BlurHits;
import com.nearinfinity.blur.hbase.BlurHits.BlurHit;
import com.nearinfinity.blur.lucene.search.SuperParser;
import com.nearinfinity.blur.utils.ForkJoin;
import com.nearinfinity.blur.utils.ForkJoin.Merger;
import com.nearinfinity.blur.utils.ForkJoin.ParallelCall;

public class SearchExecutorImpl implements SearchExecutor {

	private static final String UNKNOWN = "unknown";

	private SearchManager searchManager;
	
	public SearchExecutorImpl(SearchManager searchManager) {
		this.searchManager = searchManager;
	}

	public BlurHits search(ExecutorService executor, String query, String filter, long start, int fetchCount) {
		try {
			final Query q = parse(query);
			return ForkJoin.execute(executor, searchManager.getSearcher().entrySet(), new ParallelCall<Entry<String, Searcher>,BlurHits>() {
				@Override
				public BlurHits call(Entry<String, Searcher> input) throws Exception {
					Searcher searcher = input.getValue();
					TopDocs topDocs;
					topDocs = searcher.search((Query) q.clone(), 10);
					return convert(topDocs);
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
					return blurHits;
				}
			});
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public long searchFast(ExecutorService executor, String query, String filter, long minimum) {
		try {
			final Query q = parse(query);
			return ForkJoin.execute(executor, searchManager.getSearcher().entrySet(), new ParallelCall<Entry<String, Searcher>,Long>() {
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

	private Query parse(String query) throws ParseException {
		return new SuperParser(Version.LUCENE_CURRENT, new StandardAnalyzer(Version.LUCENE_CURRENT)).parse(query);
	}
	
	private BlurHits convert(TopDocs topDocs) {
		BlurHits blurHits = new BlurHits();
		blurHits.setTotalHits(topDocs.totalHits);
		ScoreDoc[] scoreDocs = topDocs.scoreDocs;
		for (ScoreDoc doc : scoreDocs) {
			blurHits.add(new BlurHit(doc.score, Integer.toString(doc.doc), UNKNOWN));
		}
		return blurHits;
	}

	@Override
	public void update() {
		//do nothing
	}
}
