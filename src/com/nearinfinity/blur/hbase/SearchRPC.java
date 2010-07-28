package com.nearinfinity.blur.hbase;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.io.Writable;

import com.nearinfinity.blur.hbase.ipc.SearchRegionInterface;

/**
 * Simple class for registering the search RPC codes.
 * 
 */
public final class SearchRPC {

	private static final byte RPC_CODE = 90;

	private static boolean initialized = false;

	public synchronized static void initialize() {
		if (initialized) {
			return;
		}
		HBaseRPC.addToMap(SearchRegionInterface.class, RPC_CODE);
		addWritableType(BlurHits.class);
		initialized = true;
	}

	private static void addWritableType(Class<? extends Writable> c) {
		try {
			Method method = HbaseObjectWritable.class.getDeclaredMethod("addToMap", new Class[]{Class.class,Byte.TYPE});
			method.setAccessible(true);
			method.invoke(null, new Object[]{c,(byte)0xFE});
		} catch (SecurityException e) {
			throw new RuntimeException(e);
		} catch (NoSuchMethodException e) {
			throw new RuntimeException(e);
		} catch (IllegalArgumentException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		} catch (InvocationTargetException e) {
			throw new RuntimeException(e);
		}
		
	}

	private SearchRPC() {
		// Static helper class;
	}

}
