package com.nearinfinity.blur.thrift.client;

import com.nearinfinity.blur.thrift.BlurClientManager;
import com.nearinfinity.blur.thrift.commands.BlurSearchCommand;

public class BlurClientRemote extends BlurClient {

    @Override
    public <T> T execute(String node, BlurSearchCommand<T> command) throws Exception {
        return BlurClientManager.execute(node, command);
    }

}
