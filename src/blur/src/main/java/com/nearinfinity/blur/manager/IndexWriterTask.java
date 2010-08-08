package com.nearinfinity.blur.manager;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.Directory;

import com.nearinfinity.blur.data.DataStorage;
import com.nearinfinity.blur.data.DataStorage.DataResponse;
import com.nearinfinity.blur.lucene.index.SuperDocument;
import com.nearinfinity.blur.lucene.store.ZookeeperWrapperDirectory;
import com.nearinfinity.blur.lucene.store.policy.ZookeeperIndexDeletionPolicy;
import com.nearinfinity.blur.utils.BlurConfiguration;
import com.nearinfinity.blur.utils.BlurConstants;

public class IndexWriterTask implements BlurConstants {
	
	private final static Log LOG = LogFactory.getLog(IndexWriterTask.class);
	private BlurConfiguration configuration = new BlurConfiguration();
	private Thread indexingThread;
	private volatile boolean stop;
	private long timeToWaitBetweenIndexingPasses;
	private DataStorage storage;
	private Directory directory;
	private String table;
	private String shardId;
	private DataToSuperDocumentConverter converter;
	private TableAnalyzerManager tableAnalyzerManager;
	private IndexWriter indexWriter;

	public IndexWriterTask(String table, String shardId, Directory directory, DataStorage storage) {
		this.table = table;
		this.shardId = shardId;
		this.directory = directory;
		this.storage = storage;
		this.timeToWaitBetweenIndexingPasses = configuration.getLong(BLUR_INDEXING_WAIT_TIME,10000);
		this.converter = configuration.getNewInstance(BLUR_DATATOSUPERDOCUMENT_CONVERTER_CLASS, DataToSuperDocumentConverter.class);
		this.tableAnalyzerManager = configuration.getNewInstance(BLUR_TABLEANALYZER_MANAGER_CLASS, TableAnalyzerManager.class);
	}

	public void stop() {
		stop = true;
		try {
			indexingThread.join();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	public void start() {
		indexingThread = new Thread(new Runnable() {
			@Override
			public void run() {
				openWriter();
				while (!stop) {
					try {
						Thread.sleep(timeToWaitBetweenIndexingPasses);
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
					try {
						updateIndex();
					} catch (IOException e) {
						LOG.error("unknown error",e);
					}
				}
				closeWriter();
			}


		});
	}
	
	public void openWriter() {
		if (directory instanceof ZookeeperWrapperDirectory) {
			ZookeeperWrapperDirectory dir = (ZookeeperWrapperDirectory) directory;
			try {
				IndexDeletionPolicy deletionPolicy = new ZookeeperIndexDeletionPolicy(dir.getIndexRefPath());
				Analyzer analyzer = tableAnalyzerManager.getAnalyzer(table);
				this.indexWriter =  new IndexWriter(directory, analyzer, deletionPolicy, MaxFieldLength.UNLIMITED);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		} else {
			throw new RuntimeException("Directory not supported [" + directory + "]");
		}
	}
	
	public void closeWriter() {
		try {
			indexWriter.close();
		} catch (CorruptIndexException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void updateIndex() throws IOException {
		Iterable<String> idsToBeIndexed = storage.getIdsToBeIndexed(table, shardId);
		for (String id : idsToBeIndexed) {
			DataResponse response = new DataResponse();
			storage.fetch(table, id, response);
			SuperDocument document = converter.convert(response.getMimeType(),response.getInputStream());
			for (Document doc : document.getAllDocumentsForIndexing()) {
				indexWriter.addDocument(doc);
			}
		}
		indexWriter.commit();
	}

}
