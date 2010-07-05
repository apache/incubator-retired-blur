package com.nearinfinity.blur.store.lock;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

public class ZookeeperLockFactory extends LockFactory implements Watcher {
	
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		ZooKeeper zk = new ZooKeeper("localhost", 3000, new Watcher(){
			@Override
			public void process(WatchedEvent event) {
				System.out.println(event);
			}
		});
		
		String lockDir = "/blur/lucene/locks";
		
		mkNodes(lockDir,zk);
		
		ZookeeperLock lock1 = new ZookeeperLock(zk, lockDir, "write.lock");
		ZookeeperLock lock2 = new ZookeeperLock(zk, lockDir, "write.lock");
		System.out.println("isLocked 1=" + lock1.isLocked());
		System.out.println("isLocked 2=" + lock2.isLocked());
		
		System.out.println("obtain   1=" + lock1.obtain());
		System.out.println("obtain   2=" + lock2.obtain());
		
		System.out.println("isLocked 1=" + lock1.isLocked());
		System.out.println("isLocked 2=" + lock2.isLocked());
		
		lock1.release();
		
		System.out.println("isLocked 1=" + lock1.isLocked());
		System.out.println("isLocked 2=" + lock2.isLocked());
		
	}
	
	private static final String BLUR_LUCENE_LOCKS = "/blur/lucene/locks";
	private ZooKeeper zk;
	private String lockDir = BLUR_LUCENE_LOCKS;

	public ZookeeperLockFactory(String hostname, int port) throws Exception {
		zk = new ZooKeeper("localhost", 3000, this);
		mkNodes(lockDir,zk);
	}

	public static void mkNodes(String path, ZooKeeper zk) throws KeeperException, InterruptedException {
		String[] split = path.split("/");
		for (int i = 0; i < split.length; i++) {
			StringBuilder builder = new StringBuilder();
			for (int j = 0; j <= i; j++) {
				if (!split[j].isEmpty()) {
					builder.append('/');
					builder.append(split[j]);
				}
			}
			String pathToCheck = builder.toString();
			if (pathToCheck.isEmpty()) {
				continue;
			}
			if (zk.exists(pathToCheck, false) == null) {
				zk.create(pathToCheck, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		}
	}

	@Override
	public void clearLock(String lockName) throws IOException {
		System.out.println("clear lock.... [" + lockName + "]");
	}

	@Override
	public Lock makeLock(String lockName) {
		try {
			return new ZookeeperLock(zk, lockDir, lockName);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static class ZookeeperLock extends Lock {
		
		private ZooKeeper zk;
		private String lockPath;
		private String createdLockPath;

		public ZookeeperLock(ZooKeeper zk, String lockDir, String name) throws IOException {
			this.zk = zk;
			this.lockPath = lockDir + "/" + name;
			try {
				if (zk.exists(lockPath, false) == null) {
					zk.create(lockPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}
			} catch (Exception e) {
				throw new IOException(e);
			}
		}

		@Override
		public boolean isLocked() throws IOException {
			try {
				if (zk.getChildren(lockPath, false).size() > 0) {
					return true;
				}
			} catch (Exception e) {
				throw new IOException(e);
			}
			return false;
		}

		@Override
		public boolean obtain() throws IOException {
			try {
				createdLockPath = zk.create(lockPath + "/lock-", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
				List<String> children = zk.getChildren(lockPath, false);
				if (children.size() == 1) {
					return true;
				} else {
					String lockDirName = createdLockPath.substring(lockPath.length()-2);
					for (String n : children) {
						if (lockDirName.compareTo(n) < 1) {
							zk.delete(createdLockPath, 0);
							createdLockPath = null;
							return false;
						}
					}
				}
			} catch (Exception e) {
				throw new IOException(e);
			}
			return true;
		}

		@Override
		public void release() throws IOException {
			try {
				zk.delete(createdLockPath, 0);
			} catch (Exception e) {
				throw new IOException(e);
			}
		}

	}

	@Override
	public void process(WatchedEvent arg0) {
		// TODO Auto-generated method stub
		
	}

}
