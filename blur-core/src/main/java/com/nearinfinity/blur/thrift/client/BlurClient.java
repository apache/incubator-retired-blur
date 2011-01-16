package com.nearinfinity.blur.thrift.client;

import com.nearinfinity.blur.thrift.commands.BlurSearchCommand;

public abstract class BlurClient {
    
    public abstract <T> T execute(String node, BlurSearchCommand<T> command) throws Exception;

}
