package com.nearinfinity.blur.lucene.store;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.BufferedIndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NoLockFactory;


public class BlurBaseDirectory extends Directory {

	public static final long BLOCK_SHIFT = 14; //2^14 = 16,384 bytes per block
	public static final long BLOCK_MOD = 0x3FFF;
	public static final int BLOCK_SIZE = 1 << BLOCK_SHIFT;
	
	public static long getBlock(long pos) {
		return pos >>> BLOCK_SHIFT;
	}
	
	public static long getPosition(long pos) {
		return pos & BLOCK_MOD;
	}
	
	public static long getRealPosition(long block, long positionInBlock) {
		return (block << BLOCK_SHIFT) + positionInBlock;
	}

	private DirectoryStore store;
	
	public BlurBaseDirectory(DirectoryStore store) {
		this.store = store;
		setLockFactory(new NoLockFactory());
	}
	
	@Override
	public void close() throws IOException {
		store.close();
	}

	@Override
	public void deleteFile(String name) throws IOException {
		long length = store.getFileLength(name);
		store.setFileLength(name, -1);
		long maxBlockId = getBlock(length - 1);
		for (long l = 0; l <= maxBlockId; l++) {
			store.removeBlock(name,l);
		}
	}

	@Override
	public boolean fileExists(String name) throws IOException {
		return store.fileExists(name);
	}

	@Override
	public long fileLength(String name) throws IOException {
		if (!store.fileExists(name)) {
			throw new FileNotFoundException(name);
		}
		return store.getFileLength(name);
	}

	@Override
	public long fileModified(String name) throws IOException {
		return store.getFileModified(name);
	}

	@Override
	public String[] listAll() throws IOException {
		return store.getAllFileNames().toArray(new String[]{});
	}

	@Override
	public void touchFile(String name) throws IOException {
		long fileLength = store.getFileLength(name);
		store.setFileLength(name, fileLength < 0 ? 0 : fileLength);
	}

	@Override
	public IndexOutput createOutput(final String name) throws IOException {
		store.setFileLength(name, 0);
		return new BufferedIndexOutput() {
			
			private long position;
			private long fileLength = 0;

			@Override
			public long length() throws IOException {
				return fileLength;
			}
			
			@Override
			protected void flushBuffer(byte[] b, int offset, int len) throws IOException {
				while (len > 0) {
					long blockId = getBlock(position);
					int innerPosition = (int) getPosition(position);
					byte[] block = store.fetchBlock(name,blockId);
					if (block == null) {
						block = new byte[BLOCK_SIZE];
					}
					int length = Math.min(len, block.length - innerPosition);
					System.arraycopy(b, offset, block, innerPosition, length);
					store.saveBlock(name,blockId,block);
					position += length;
					len -= length;
					offset += length;
				}
				if (position > fileLength) {
					setLength(position);
				}
			}

			@Override
			public void close() throws IOException {
				super.close();
				store.flush(name);
			}

			@Override
			public void seek(long pos) throws IOException {
				super.seek(pos);
				this.position = pos;
			}

			@Override
			public void setLength(final long length) throws IOException {
				super.setLength(length);
				fileLength = length;
				store.setFileLength(name,length);
			}
		};
	}


	@Override
	public IndexInput openInput(final String name) throws IOException {
		if (!fileExists(name)) {
			touchFile(name);
		}
		final long fileLength = fileLength(name);
		return new BufferedIndexInput(BLOCK_SIZE) {
			@Override
			public long length() {
				return fileLength;
			}
			
			@Override
			public void close() throws IOException {
				
			}
			
			@Override
			protected void seekInternal(long pos) throws IOException {
			}
			
			@Override
			protected void readInternal(byte[] b, int off, int len) throws IOException {
				long position = getFilePointer();
				while (len > 0) {
					long blockId = getBlock(position);
					int innerPosition = (int) getPosition(position);
					byte[] block = store.fetchBlock(name, blockId);
					int length = Math.min(len,block.length-innerPosition);
					System.arraycopy(block, innerPosition, b, off, length);
					position += length;
					len -= length;
					off += length;
				}
			}
		};
	}
}
