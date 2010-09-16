package com.nearinfinity.blur.utils;

import com.nearinfinity.mele.MeleConfiguration;

public class BlurConfiguration extends MeleConfiguration implements BlurConstants {
    
    public BlurConfiguration() {
        super();
        addResource("blur-default.xml");
        addResource("blur-site.xml");
    }

    public int getBlurShardServerPort() {
        return getInt(BLUR_SERVER_SHARD_PORT,BLUR_SERVER_SHARD_PORT_DEFAULT);
    }
    
    public void setBlurShardServerPort(int port) {
        setInt(BLUR_SERVER_SHARD_PORT, port);
    }

    public int getBlurControllerServerPort() {
        return getInt(BLUR_SERVER_CONTROLLER_PORT,BLUR_SERVER_CONTROLLER_PORT_DEFAULT);
    }
    
    public void setBlurControllerServerPort(int port) {
        setInt(BLUR_SERVER_CONTROLLER_PORT, port);
    }

}
