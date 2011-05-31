package com.nearinfinity.blur.thrift;

import org.apache.thrift.TException;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.manager.indexserver.DistributedManager;
import com.nearinfinity.blur.manager.indexserver.utils.CreateTable;
import com.nearinfinity.blur.manager.indexserver.utils.DisableTable;
import com.nearinfinity.blur.manager.indexserver.utils.EnableTable;
import com.nearinfinity.blur.manager.indexserver.utils.RemoveTable;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;

public abstract class TableAdmin implements Iface {
    
    private static final Log LOG = LogFactory.getLog(TableAdmin.class);
    
    protected DistributedManager _dm;

    @Override
    public void createTable(String table, TableDescriptor tableDescriptor) throws BlurException, TException {
        try {
            BlurAnalyzer analyzer = new BlurAnalyzer(tableDescriptor.analyzerDefinition);
            CreateTable.createTable(_dm,table,analyzer,tableDescriptor.tableUri,
                    tableDescriptor.shardCount,CreateTable.getInstance(tableDescriptor.compressionClass),
                    tableDescriptor.compressionBlockSize);
        } catch (Exception e) {
            LOG.error("Unknown error during create of [table={0}, tableDescriptor={1}]", e, table, tableDescriptor);
            throw new BException(e.getMessage(), e);
        }
        if (tableDescriptor.isEnabled) {
            enableTable(table);
        }
    }

    @Override
    public void disableTable(String table) throws BlurException, TException {
        try {
            DisableTable.disableTable(_dm, table);
        } catch (Exception e) {
            LOG.error("Unknown error during disable of [table={0}]", e, table);
            throw new BException(e.getMessage(), e);
        }
    }

    @Override
    public void enableTable(String table) throws BlurException, TException {
        try {
            EnableTable.enableTable(_dm, table);
        } catch (Exception e) {
            LOG.error("Unknown error during enable of [table={0}]", e, table);
            throw new BException(e.getMessage(), e);
        }
    }

    @Override
    public void removeTable(String table, boolean deleteIndexFiles) throws BlurException, TException {
        try {
            RemoveTable.removeTable(_dm, table, deleteIndexFiles);
        } catch (Exception e) {
            LOG.error("Unknown error during remove of [table={0}]", e, table);
            throw new BException(e.getMessage(), e);
        }
    }

    public void setDistributedManager(DistributedManager distributedManager) {
        _dm = distributedManager;
    }
    
    
    
}
