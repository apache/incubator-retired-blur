package com.nearinfinity.blur.thrift;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;

import com.nearinfinity.blur.manager.IndexManager;
import com.nearinfinity.blur.manager.IndexManager.TableManager;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.Hits;
import com.nearinfinity.blur.thrift.generated.MissingShardException;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.ScoreType;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.utils.BlurConstants;

public class BlurShardServer extends BlurAdminServer implements BlurConstants {

	private static final Log LOG = LogFactory.getLog(BlurShardServer.class);
	private IndexManager indexManager;
	
	public BlurShardServer() throws IOException, BlurException {
		super();
		indexManager = new IndexManager(new TableManager() {
			
			@Override
			public boolean isTableEnabled(String table) {
				try {
					TableDescriptor describe = describe(table);
					System.out.println("isTableEnabled " + table + " = " + describe);
					if (describe == null) {
						return false;
					}
					return describe.isEnabled;
				} catch (Exception e) {
				    LOG.error("Uknown error while trying to check if table [" + table +
				    		"] is enabled.",e);
					throw new RuntimeException(e);
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
	public Hits search(String table, String query, boolean superQueryOn, ScoreType type, String postSuperFilter, String preSuperFilter, 
			final long start, final int fetch, long minimumNumberOfHits, long maxQueryTime) throws BlurException, TException {
		return indexManager.search(table, query, superQueryOn, type, postSuperFilter, preSuperFilter, start, fetch, minimumNumberOfHits, maxQueryTime);
	}
	
	@Override
	protected NODE_TYPE getType() {
		return NODE_TYPE.NODE;
	}

	@Override
	public Row fetchRow(String table, String id) throws BlurException, TException, MissingShardException {
		return indexManager.fetchRow(table,id);
	}

	@Override
	public void appendRow(String table, Row row) throws BlurException, TException {
		indexManager.appendRow(table,row);
	}

	@Override
	public void removeRow(String table, String id) throws BlurException, TException {
		indexManager.removeRow(table,id);
	}

	@Override
	public void replaceRow(String table, Row row) throws BlurException, TException, MissingShardException {
		indexManager.replaceRow(table,row);
	}
}
