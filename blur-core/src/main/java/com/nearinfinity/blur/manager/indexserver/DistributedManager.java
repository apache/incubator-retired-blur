package com.nearinfinity.blur.manager.indexserver;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class DistributedManager {
    
    public static class Value {
        public int version;
        public byte[] data;
    }

    public abstract void close();
    protected abstract boolean existsInternal(String path);
    protected abstract void lockInternal(String path);
    protected abstract void unlockInternal(String path);
    protected abstract void fetchDataInternal(Value value, String path);
    protected abstract boolean saveDataInternal(byte[] data, String path);
    protected abstract void createPathInternal(String path);
    protected abstract void createEphemeralPathInternal(String path);
    protected abstract void removeEphemeralPathOnShutdownInternal(String path);
    protected abstract void removePath(String path);
    protected abstract List<String> listInternal(String path);
    protected abstract void registerCallableOnChangeInternal(Runnable runnable, String path);
    
    public void lock(String... pathes) {
        lockInternal(resolvePath(pathes));
    }
    
    public void unlock(String... pathes) {
        unlockInternal(resolvePath(pathes));
    }
    
    public boolean saveData(byte[] data, String... pathes) {
        return saveDataInternal(data, resolvePath(pathes));
    }
    
    public void fetchData(Value value, String... pathes) {
        fetchDataInternal(value, resolvePath(pathes));
    }

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
    
    public void removeEphemeralPathOnShutdown(String... pathes) {
        removeEphemeralPathOnShutdownInternal(resolvePath(pathes));
    }
    
    public void removePath(String... pathes) {
        removePath(resolvePath(pathes));
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
