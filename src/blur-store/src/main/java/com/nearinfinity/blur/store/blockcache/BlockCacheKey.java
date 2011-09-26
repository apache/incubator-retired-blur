package com.nearinfinity.blur.store.blockcache;

public class BlockCacheKey implements Cloneable {

    private long _block;
    private int _file;

    public long getBlock() {
        return _block;
    }

    public int getFile() {
        return _file;
    }
    
    public void setBlock(long block) {
        _block = block;
    }

    public void setFile(int file) {
        _file = file;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (_block ^ (_block >>> 32));
        result = prime * result + _file;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        BlockCacheKey other = (BlockCacheKey) obj;
        if (_block != other._block)
            return false;
        if (_file != other._file)
            return false;
        return true;
    }

    @Override
    public BlockCacheKey clone() {
        try {
            return (BlockCacheKey) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }
}













