package com.nearinfinity.blur.manager.indexserver;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

public abstract class Zk {

    public abstract void close();
    protected abstract boolean existsInternal(String path);
    protected abstract void createPathInternal(String path);
    protected abstract void createEphemeralPathInternal(String path);
    protected abstract List<String> listInternal(String path);
    protected abstract void registerCallableOnChangeInternal(Callable<?> n, String path);

    public boolean exists(String... pathes) {
        return existsInternal(resolvePath(pathes));
    }

    public void createPath(String... pathes) {
        createPathInternal(resolvePath(pathes));
    }

    public void createEphemeralPath(String... pathes) {
        createEphemeralPathInternal(resolvePath(pathes));
    }

    public List<String> list(String... pathes) {
        return listInternal(resolvePath(pathes));
    }

    public void registerCallableOnChange(Callable<?> n, String... pathes) {
        registerCallableOnChangeInternal(n,resolvePath(pathes));
    }

    private String resolvePath(String[] pathes) {
        List<String> path = new ArrayList<String>();
        for (int i = 0; i < pathes.length; i++) {
            path.addAll(getParts(pathes[i]));
        }
        return join(path);
    }
    
    private String join(List<String> path) {
        StringBuilder builder = new StringBuilder();
        for (String p : path) {
            builder.append('/').append(p);
        }
        return builder.toString();
    }
    
    private List<String> getParts(String path) {
        return Arrays.asList(path.split("\\/"));
    }

}
