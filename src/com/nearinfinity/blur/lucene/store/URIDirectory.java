package com.nearinfinity.blur.lucene.store;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import com.nearinfinity.blur.lucene.store.dao.hbase.HbaseDao;

public class URIDirectory {
	
	public static interface OpenDirectory {
		Directory open(URI uri) throws IOException;
	}
	
	private URIDirectory() {}
	private static Map<String, OpenDirectory> openDirs;
	
	static {
		openDirs = new ConcurrentHashMap<String, OpenDirectory>();
		register("hbase", new OpenDirectory() {
			@Override
			public Directory open(URI uri) throws IOException {
				String path = uri.getPath();
				String[] split = path.split("/");
				String tableName = split[1];
				String columnFamily = split[2];
				String dirName = split[2];
				return new BlurBaseDirectory(new HbaseDao(tableName, columnFamily, dirName));
			}
		});
		register("file", new OpenDirectory() {
			@Override
			public Directory open(URI uri) throws IOException {
				return FSDirectory.open(new File(uri.getPath()));
			}
		});
		register("zk", new OpenDirectory() {
			@Override
			public Directory open(URI uri) throws IOException {
				URI subDirUri = getSubDirUri(uri.getQuery());
				return new ZookeeperWrapperDirectory(openDirectory(subDirUri),uri.getPath());
			}

			private URI getSubDirUri(String query) {
				try {
					return new URI(query);
				} catch (URISyntaxException e) {
					throw new RuntimeException(e);
				}
			}
		});
	}
	
	public static Directory openDirectory(URI uri) throws IOException {
		OpenDirectory openDirectory = openDirs.get(uri.getScheme());
		if (openDirectory == null) {
			throw new IllegalArgumentException("No OpenDirectory registered for uri [" + uri + "]");
		}
		return openDirectory.open(uri);
	}
	
	public static void register(String scheme, OpenDirectory openDirectory) {
		openDirs.put(scheme, openDirectory);
	}
}
