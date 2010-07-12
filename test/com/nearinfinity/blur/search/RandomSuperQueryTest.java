package com.nearinfinity.blur.search;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import com.nearinfinity.blur.index.SuperDocument;
import com.nearinfinity.blur.index.SuperIndexReader;
import com.nearinfinity.blur.index.SuperIndexWriter;

public class RandomSuperQueryTest extends TestCase {
	
	private static final int MOD_COLS_USED_FOR_SKIPPING = 3;
	private static final int MAX_NUM_OF_DOCS = 100;
	private static final int MIN_NUM_COL_FAM = 3;
	private static final int MAX_NUM_COL_FAM = 10;
	private static final int MAX_NUM_DOCS_PER_COL_FAM = 10;
	private static final int MAX_NUM_COLS = 10;
	private static final int MIN_NUM_COLS = 1;
	private static final int MAX_NUM_OF_WORDS = 25000;
	private static final int MOD_USED_FOR_SAMPLING = 7;
	
	private Random seedGen = new Random();
	
	public void testRandomSuperQuery() throws CorruptIndexException, IOException, InterruptedException, ParseException {
//		long seed = seedGen.nextLong();
		long seed = -8230906496977599182L;

		Random random = new Random(seed);
		Collection<String> sampler = new HashSet<String>();
		Directory directory = createIndex(random, sampler);
		IndexReader reader = IndexReader.open(directory);
		SuperIndexReader indexReader = new SuperIndexReader(reader);
		indexReader.waitForWarmUp();
		SuperSearcher searcher = new SuperSearcher(indexReader);
		for (String s : sampler) {
			System.out.println(s);
			Query query = new SuperParser(Version.LUCENE_CURRENT, new StandardAnalyzer(Version.LUCENE_CURRENT)).parse(s);
			TopDocs topDocs = searcher.search(query, 10);
			assertTrue("seed [" + seed + "] {" + query + "} {" + s + "}",topDocs.totalHits > 0);
		}
	}

	private Directory createIndex(Random random, Collection<String> sampler) throws CorruptIndexException, LockObtainFailedException, IOException {
		Directory directory = new RAMDirectory();
		String[] columnFamilies = genWords(random,MIN_NUM_COL_FAM,MAX_NUM_COL_FAM,"colfam");
		Map<String,String[]> columns = new HashMap<String,String[]>();
		for (int i = 0; i < columnFamilies.length; i++) {
			columns.put(columnFamilies[i], genWords(random,MIN_NUM_COLS,MAX_NUM_COLS,"col"));
		}
		
		SuperIndexWriter writer = new SuperIndexWriter(directory, new StandardAnalyzer(Version.LUCENE_CURRENT), MaxFieldLength.UNLIMITED);
		int numberOfDocs = random.nextInt(MAX_NUM_OF_DOCS) + 1;
		for (int i = 0; i < numberOfDocs; i++) {
			writer.addSuperDocument(generatSuperDoc(random, columns, sampler));
		}
		writer.close();
		return directory;
	}

	private String[] genWords(Random random, int min, int max, String prefix) {
		int numberOfColFam = random.nextInt(max - min) + min;
		String[] str = new String[numberOfColFam];
		for (int i = 0; i < numberOfColFam; i++) {
			str[i] = genWord(random, prefix);
		}
		return str;
	}

	private SuperDocument generatSuperDoc(Random random, Map<String, String[]> columns, Collection<String> sampler) {
		SuperDocument document = new SuperDocument(Long.toString(random.nextLong()));
		StringBuilder builder = new StringBuilder();
		for (String colFam : columns.keySet()) {
			String[] cols = columns.get(colFam);
			for (int i = 0; i < random.nextInt(MAX_NUM_DOCS_PER_COL_FAM); i++) {
				String superKey = Long.toString(random.nextLong());
				for (String column : cols) {
					if (random.nextInt() % MOD_COLS_USED_FOR_SKIPPING == 0) {
						String word = genWord(random,"word");
						document.addFieldAnalyzedNoNorms(colFam, superKey, column, word);
						if (random.nextInt() % MOD_USED_FOR_SAMPLING == 0) {
							builder.append(" +" + colFam + "." + column + ":" + word);
						}
					}
				}
			}
		}
		String string = builder.toString();
		if (!string.isEmpty()) {
			sampler.add(string);
		}
		return document;
	}
	
	private String genWord(Random random, String prefix) {
		return prefix + random.nextInt(MAX_NUM_OF_WORDS);
	}
}
