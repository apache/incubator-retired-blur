package com.nearinfinity.blur.manager;

import com.nearinfinity.blur.thrift.generated.Transaction;

public class TransactionManager {
    
    public Transaction create(String table, int shards) {
        return null;
    }

    public void commit(Transaction transaction) {
        //tell everyone to commit
        //wait for everyone to complete phase 1
        //if all are in commit state
        //  commit phase 2
        //else 
        //  abort phase 1
    }

    public void abort(Transaction transaction) {
        
    }

    public void commit(Transaction transaction, String shard) {
        
    }
    

}
