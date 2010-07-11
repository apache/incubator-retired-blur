package com.nearinfinity.blur.store.policy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

import com.nearinfinity.blur.utils.ZkUtils;

public class ZookeeperIndexDeletionPolicy implements IndexDeletionPolicy {

	private static final Log LOG = LogFactory.getLog(ZookeeperIndexDeletionPolicy.class);
	private String indexRefPath;
	private ZooKeeper zk;

	public ZookeeperIndexDeletionPolicy(ZooKeeper zk, String indexRefPath) throws Exception {
		this.zk = zk;
		this.indexRefPath = indexRefPath;
		ZkUtils.mkNodes(indexRefPath, zk);
	}

	@Override
	public void onCommit(List<? extends IndexCommit> commits) throws IOException {
		List<String> filesCurrentlyBeingReferenced = getListOfReferencedFiles();
		int size = commits.size();
		OUTER: for (int i = 0; i < size - 1; i++) {
			IndexCommit indexCommit = commits.get(i);
			Collection<String> fileNames = indexCommit.getFileNames();
			for (String fileName : fileNames) {
				if (filesCurrentlyBeingReferenced.contains(fileName)) {
//					LOG.info("Indexcommit [" + indexCommit + "] cannot be reclaimed because file [" + fileName + "] is still being referenced in a live index reader.");
					continue OUTER;
				}
			}
			System.out.println("deleting files [" + fileNames + "]");
			indexCommit.delete();
		}
	}

	@Override
	public void onInit(List<? extends IndexCommit> commits) throws IOException {
		onCommit(commits);
	}

	private List<String> getListOfReferencedFiles() {
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
	
	private String getName(String name) {
		int index = name.lastIndexOf('.');
		return name.substring(0,index);
	}

	public static String createRef(ZooKeeper zk, String indexRefPath, String name) {
		try {
			return zk.create(indexRefPath + "/" + name + ".", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		} catch (KeeperException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	public static void removeRef(ZooKeeper zk, String refPath) {
		try {
			zk.delete(refPath, 0);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (KeeperException e) {
			throw new RuntimeException(e);
		}
	}
}
