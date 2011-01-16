package com.nearinfinity.blur.thrift;

public abstract class AbstractCommand<CLIENT,T> implements Cloneable {
    public abstract T call(CLIENT client) throws Exception;

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
