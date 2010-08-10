package com.nearinfinity.blur.lucene.store;

import java.io.IOException;
import java.util.List;

public interface DirectoryStore {
	
	/**
	 * Saves the block of file data to a persistent store.
	 * @param name the name of the file to save.
	 * @param blockId the block id to save.
	 * @param block the binary data to save.
	 * @throws IOException
	 */
	void saveBlock(String name, long blockId, byte[] block) throws IOException;
	
	/**
	 * Fetches a block of data from the persistent store.
	 * @param name the name of the file to fetch.
	 * @param blockId the the block id to fetch.
	 * @return the binary data of the block.
	 * @throws IOException
	 */
	byte[] fetchBlock(String name, long blockId) throws IOException;
	
	/**
	 * Lists all available files in this directory.
	 * @return the list of all the files in this directory.
	 * @throws IOException
	 */
	List<String> getAllFileNames() throws IOException;
	
	/**
	 * Checks to see if a file exists.
	 * @param name the file to check.
	 * @return boolean.
	 * @throws IOException
	 */
	boolean fileExists(String name) throws IOException;
	
	/**
	 * Gets last time the file was modified.
	 * @param name the file name.
	 * @return the last modified time stamp.
	 * @throws IOException
	 */
	long getFileModified(String name) throws IOException;
	
	/**
	 * Gets the file length.
	 * @param name the file name.
	 * @return the file length.
	 * @throws IOException
	 */
	long getFileLength(String name) throws IOException;
	
	/**
	 * Sets file length.
	 * @param name the file name.
	 * @param length the file length.
	 * @throws IOException
	 */
	void setFileLength(String name, long length) throws IOException;
	
	/**
	 * Closes this directory data access object.
	 */
	void close() throws IOException;

	/**
	 * Flushes the file to persistent store.
	 * @param name
	 * @throws IOException
	 */
	void flush(String name) throws IOException;

	void removeBlock(String name, long blockId) throws IOException;
	
	void removeFileMetaData(String name) throws IOException;

}
