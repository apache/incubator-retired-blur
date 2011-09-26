package com.nearinfinity.blur.store.blockcache;


public class BlockCacheLocation {

    private int _block;
    private int _bankId;
    private long _lastAccess = System.currentTimeMillis();
    private long _accesses;
    
    public void setBlock(int block) {
        _block = block;
    }

    public void setBankId(int bankId) {
        _bankId = bankId;
    }

    public int getBlock() {
        return _block;
    }

    public int getBankId() {
        return _bankId;
    }

    public void touch() {
        _lastAccess = System.currentTimeMillis();
        _accesses++;
    }
    
    public long getLastAccess() {
        return _lastAccess;
    }

    public long getNumberOfAccesses() {
        return _accesses;
    }

}