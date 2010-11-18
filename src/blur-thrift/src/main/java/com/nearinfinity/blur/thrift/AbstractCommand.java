package com.nearinfinity.blur.thrift;

public abstract class AbstractCommand<CLIENT,T> {
    public abstract T call(CLIENT client) throws Exception;
}
