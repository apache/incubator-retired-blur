package com.nearinfinity.blur.utils;

import java.io.IOException;

import com.nearinfinity.mele.MeleConfiguration;

public class BlurConfiguration extends MeleConfiguration implements BlurConstants {
    
    private static final long serialVersionUID = -2541300525074540373L;

    public BlurConfiguration() throws IOException {
        super();
        addResource("blur-default.properties");
        addResource("blur-site.properties");
    }

    public int getBlurShardServerPort() {
        return getPropertyInt(BLUR_SERVER_SHARD_PORT,BLUR_SERVER_SHARD_PORT_DEFAULT);
    }
    
    public void setBlurShardServerPort(int port) {
        setPropertyInt(BLUR_SERVER_SHARD_PORT, port);
    }

    public int getBlurControllerServerPort() {
        return getPropertyInt(BLUR_SERVER_CONTROLLER_PORT,BLUR_SERVER_CONTROLLER_PORT_DEFAULT);
    }
    
    public void setBlurControllerServerPort(int port) {
        setPropertyInt(BLUR_SERVER_CONTROLLER_PORT, port);
    }

}
