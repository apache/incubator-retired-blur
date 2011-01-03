package com.nearinfinity.blur.manager.indexserver;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class DistributedManager {

    public abstract void close();
    protected abstract boolean existsInternal(String path);
    protected abstract void createPathInternal(String path);
    protected abstract void createEphemeralPathInternal(String path);
    protected abstract List<String> listInternal(String path);
    protected abstract void registerCallableOnChangeInternal(Runnable runnable, String path);

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

    public void registerCallableOnChange(Runnable runnable, String... pathes) {
        registerCallableOnChangeInternal(runnable,resolvePath(pathes));
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
            if (isEmpty(p)) {
                continue;
            }
            builder.append('/').append(p);
        }
        return builder.toString();
    }
    
    private boolean isEmpty(String p) {
        if (p == null || p.trim().equals("")) {
            return true;
        }
        return false;
    }
    private List<String> getParts(String path) {
        return Arrays.asList(path.split("\\/"));
    }

}
