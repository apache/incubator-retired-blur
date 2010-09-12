package com.nearinfinity.blur.thrift;

import java.io.IOException;

import org.apache.thrift.TException;

import com.nearinfinity.blur.manager.IndexManager;
import com.nearinfinity.blur.manager.IndexManager.TableManager;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.Hits;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.ScoreType;
import com.nearinfinity.blur.thrift.generated.SuperColumn;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.utils.BlurConstants;

public class BlurShardServer extends BlurAdminServer implements BlurConstants {

//	private static final Log LOG = LogFactory.getLog(BlurShardServer.class);
	private IndexManager indexManager;
	
	public BlurShardServer() throws IOException, BlurException {
		super();
		indexManager = new IndexManager(new TableManager() {
			
			@Override
			public boolean isTableEnabled(String table) {
				try {
					TableDescriptor describe = describe(table);
					if (describe == null) {
						return false;
					}
					return describe.isEnabled;
				} catch (Exception e) {
					throw new RuntimeException();
				}
			}
			
			@Override
			public String getAnalyzerDefinitions(String table) {
				try {
					TableDescriptor describe = describe(table);
					if (describe == null) {
						return "";
					}
					return describe.analyzerDef;
				} catch (Exception e) {
					throw new RuntimeException();
				}
			}
		});
	}

	@Override
	public Hits search(String table, String query, boolean superQueryOn, ScoreType type, String filter, 
			final long start, final int fetch, long minimumNumberOfHits, long maxQueryTime) throws BlurException, TException {
		return indexManager.search(table, query, superQueryOn, type, filter, start, fetch, minimumNumberOfHits, maxQueryTime);
	}
	
	@Override
	protected NODE_TYPE getType() {
		return NODE_TYPE.NODE;
	}

	@Override
	public Row fetchRow(String table, String id) throws BlurException, TException {
		return indexManager.fetchRow(table,id);
	}

	@Override
	public SuperColumn fetchSuperColumn(String table, String id, String superColumnFamilyName, String superColumnId) throws BlurException, TException {
		return indexManager.fetchSuperColumn(table,id,superColumnFamilyName,superColumnId);
	}
	
	@Override
	public boolean appendRow(String table, Row row) throws BlurException, TException {
		return indexManager.appendRow(table,row);
	}

	@Override
	public boolean removeRow(String table, String id) throws BlurException, TException {
		return indexManager.removeRow(table,id);
	}

	@Override
	public boolean removeSuperColumn(String table, String id, String superColumnId) throws BlurException, TException {
		return indexManager.removeSuperColumn(table,id,superColumnId);
	}

	@Override
	public boolean replaceRow(String table, Row row) throws BlurException, TException {
		return indexManager.replaceRow(table,row);
	}
}
