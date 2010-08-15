package com.nearinfinity.blur.lucene.store.policy;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nearinfinity.blur.utils.ZkUtils;
import com.nearinfinity.blur.zookeeper.ZooKeeperFactory;

public class ZookeeperIndexDeletionPolicy implements IndexDeletionPolicy {

	private final static Logger LOG = LoggerFactory.getLogger(ZookeeperIndexDeletionPolicy.class);
	private String indexRefPath;
	private ZooKeeper zk;

	public ZookeeperIndexDeletionPolicy(String indexRefPath) throws Exception {
		this.zk = ZooKeeperFactory.getZooKeeper();
		this.indexRefPath = indexRefPath;
		ZkUtils.mkNodes(indexRefPath, zk);
	}

	public ZookeeperIndexDeletionPolicy(URI uri) throws Exception {
		this(uri.getPath());
	}

	@Override
	public void onCommit(List<? extends IndexCommit> commits) throws IOException {
		List<String> filesCurrentlyBeingReferenced = getListOfReferencedFiles(zk,indexRefPath);
		int size = commits.size();
		Collection<String> previouslyReferencedFiles = new TreeSet<String>();
		OUTER: for (int i = size - 2; i >= 0; i--) {
			IndexCommit indexCommit = commits.get(i);
			LOG.info("Processing index commit generation {}", indexCommit.getGeneration());
			Collection<String> fileNames = new TreeSet<String>(indexCommit.getFileNames());
			//remove all filenames that were references in newer index commits,
			//this way older index commits can be released without the fear of 
			//broken references.
			fileNames.removeAll(previouslyReferencedFiles);
			for (String fileName : fileNames) {
				if (filesCurrentlyBeingReferenced.contains(fileName)) {
					previouslyReferencedFiles.addAll(fileNames);
					continue OUTER;
				}
			}
			LOG.info("Index Commit {} no longer needed, releasing {}",indexCommit.getGeneration(),fileNames);
			indexCommit.delete();
		}
	}

	@Override
	public void onInit(List<? extends IndexCommit> commits) throws IOException {
		onCommit(commits);
	}

	public static List<String> getListOfReferencedFiles(ZooKeeper zk, String indexRefPath) {
		try {
			List<String> files = new ArrayList<String>();
			List<String> children = zk.getChildren(indexRefPath, false);
			for (String child : children) {
				String name = getName(child);
				if (!files.contains(name)) {
					files.add(name);
				}
			}
			return files;
		} catch (KeeperException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
	
	private static String getName(String name) {
		int index = name.lastIndexOf('.');
		return name.substring(0,index);
	}

	public static String createRef(ZooKeeper zk, String indexRefPath, String name) {
		try {
			String path = zk.create(indexRefPath + "/" + name + ".", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			LOG.debug("Created reference path {}",path);
			return path;
		} catch (KeeperException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	public static void removeRef(ZooKeeper zk, String refPath) {
		try {
			LOG.debug("Removing reference path {}",refPath);
			zk.delete(refPath, 0);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (KeeperException e) {
			throw new RuntimeException(e);
		}
	}
}
