package com.nearinfinity.blur.lucene.store;

import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.blur.lucene.store.lock.ZookeeperLockFactory;
import com.nearinfinity.blur.lucene.store.policy.ZookeeperIndexDeletionPolicy;
import com.nearinfinity.blur.utils.ZkUtils;

public class ZookeeperWrapperDirectory extends Directory {
	
	private Directory directory;
	private ZooKeeper zk;
	private String indexRefPath;
	
	public ZookeeperWrapperDirectory(ZooKeeper zk, Directory directory, String indexRefPath) throws IOException {
		this.directory = directory;
		this.indexRefPath = indexRefPath;
		this.zk = zk;
		ZkUtils.mkNodes(indexRefPath, zk);
		this.setLockFactory(new ZookeeperLockFactory(zk));
	}

	public void clearLock(String name) throws IOException {
		directory.clearLock(name);
	}

	public void close() throws IOException {
		directory.close();
	}

	public IndexOutput createOutput(String name) throws IOException {
		return directory.createOutput(name);
	}

	public void deleteFile(String name) throws IOException {
		directory.deleteFile(name);
	}

	public boolean equals(Object obj) {
		return directory.equals(obj);
	}

	public boolean fileExists(String name) throws IOException {
		return directory.fileExists(name);
	}

	public long fileLength(String name) throws IOException {
		return directory.fileLength(name);
	}

	public long fileModified(String name) throws IOException {
		return directory.fileModified(name);
	}

	public LockFactory getLockFactory() {
		return directory.getLockFactory();
	}

	public String getLockID() {
		return directory.getLockID();
	}

	public int hashCode() {
		return directory.hashCode();
	}

	public String[] listAll() throws IOException {
		return directory.listAll();
	}

	public Lock makeLock(String name) {
		return directory.makeLock(name);
	}

	public IndexInput openInput(String name, int bufferSize) throws IOException {
		return wrapRef(name, directory.openInput(name, bufferSize));
	}

	public IndexInput openInput(String name) throws IOException {
		return wrapRef(name, directory.openInput(name));
	}

	public void setLockFactory(LockFactory lockFactory) {
		directory.setLockFactory(lockFactory);
	}

	public void sync(String name) throws IOException {
		directory.sync(name);
	}

	public String toString() {
		return directory.toString();
	}

	public void touchFile(String name) throws IOException {
		directory.touchFile(name);
	}

	private IndexInput wrapRef(final String name, final IndexInput indexInput) {
		final String refPath = ZookeeperIndexDeletionPolicy.createRef(zk,indexRefPath,name);
		return new IndexInput() {
			
			@Override
			public void close() throws IOException {
				indexInput.close();
				ZookeeperIndexDeletionPolicy.removeRef(zk,refPath);
			}

			@Override
			public long getFilePointer() {
				return indexInput.getFilePointer();
			}

			@Override
			public long length() {
				return indexInput.length();
			}

			@Override
			public byte readByte() throws IOException {
				return indexInput.readByte();
			}

			@Override
			public void readBytes(byte[] b, int offset, int len) throws IOException {
				indexInput.readBytes(b, offset, len);
			}

			@Override
			public void seek(long pos) throws IOException {
				indexInput.seek(pos);
			}

			@Override
			public Object clone() {
				return indexInput.clone();
			}
		};
	}
}
