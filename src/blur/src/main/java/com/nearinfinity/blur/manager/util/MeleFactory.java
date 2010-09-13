package com.nearinfinity.blur.manager.util;

import java.io.IOException;

import com.nearinfinity.mele.Mele;
import com.nearinfinity.mele.MeleConfiguration;

public class MeleFactory {
    
    private static Mele mele;
    private static MeleConfiguration meleConfiguration = new MeleConfiguration();
    
    public static void setup(MeleConfiguration configuration) {
        meleConfiguration = configuration;
    }
    
    public synchronized static Mele getInstance() throws IOException {
        if (mele == null) {
            mele = new Mele(meleConfiguration);
        }
        return mele;
    }

}
