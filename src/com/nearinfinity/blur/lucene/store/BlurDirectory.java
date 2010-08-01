package com.nearinfinity.blur.lucene.store;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.BufferedIndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NoLockFactory;

import com.nearinfinity.blur.lucene.store.dao.DirectoryDao;

public class BlurDirectory extends Directory {

	public static final long BLOCK_SHIFT = 14; //2^19 = 524,288 bits which is 65,536 bytes per block
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

	private DirectoryDao dao;
	
	public BlurDirectory(DirectoryDao dao) {
		this.dao = dao;
		setLockFactory(new NoLockFactory());
	}
	
	@Override
	public void close() throws IOException {
		dao.close();
	}

	@Override
	public void deleteFile(String name) throws IOException {
		long length = dao.getFileLength(name);
		dao.setFileLength(name, -1);
		long maxBlockId = getBlock(length - 1);
		for (long l = 0; l <= maxBlockId; l++) {
			dao.removeBlock(name,l);
		}
	}

	@Override
	public boolean fileExists(String name) throws IOException {
		return dao.fileExists(name);
	}

	@Override
	public long fileLength(String name) throws IOException {
		if (!dao.fileExists(name)) {
			throw new FileNotFoundException(name);
		}
		return dao.getFileLength(name);
	}

	@Override
	public long fileModified(String name) throws IOException {
		return dao.getFileModified(name);
	}

	@Override
	public String[] listAll() throws IOException {
		return dao.getAllFileNames().toArray(new String[]{});
	}

	@Override
	public void touchFile(String name) throws IOException {
		long fileLength = dao.getFileLength(name);
		dao.setFileLength(name, fileLength < 0 ? 0 : fileLength);
	}

	@Override
	public IndexOutput createOutput(final String name) throws IOException {
		dao.setFileLength(name, 0);
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
					byte[] block = dao.fetchBlock(name,blockId);
					if (block == null) {
						block = new byte[BLOCK_SIZE];
					}
					int length = Math.min(len, block.length - innerPosition);
					System.arraycopy(b, offset, block, innerPosition, length);
					dao.saveBlock(name,blockId,block);
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
				dao.flush(name);
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
				dao.setFileLength(name,length);
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
					byte[] block = dao.fetchBlock(name, blockId);
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
