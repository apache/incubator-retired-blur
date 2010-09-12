package com.nearinfinity.blur.lucene.wal;

import java.io.IOException;

import org.apache.lucene.index.IndexWriter;

import com.nearinfinity.blur.manager.Partitioner;
import com.nearinfinity.blur.thrift.generated.Row;

public abstract class BlurWriteAheadLog {
	
	public abstract void replay(String table, String shard, Partitioner partitioner, IndexWriter indexWriter) throws IOException;

	public abstract void appendRow(String table, Row row) throws IOException;

	public abstract void replaceRow(String table, Row row) throws IOException;

	public abstract void removeSuperColumn(String table, String id, String superColumnId) throws IOException;

	public abstract void removeRow(String table, String id) throws IOException;

	public abstract void commit(String table, String shard, IndexWriter indexWriter) throws IOException;
	
	public static final BlurWriteAheadLog NO_LOG = new BlurWriteAheadLog() {
		
		@Override
		public void replay(String table, String shard, Partitioner partitioner,
				IndexWriter indexWriter) throws IOException {
			
		}
		
		@Override
		public void replaceRow(String table, Row row) throws IOException {
			
		}
		
		@Override
		public void removeSuperColumn(String table, String id, String superColumnId) throws IOException {
			
		}
		
		@Override
		public void removeRow(String table, String id) throws IOException {
			
		}
		
		@Override
		public void commit(String table, String shard, IndexWriter indexWriter) throws IOException {
			indexWriter.commit();
		}
		
		@Override
		public void appendRow(String table, Row row) throws IOException {
			
		}
	};
}
