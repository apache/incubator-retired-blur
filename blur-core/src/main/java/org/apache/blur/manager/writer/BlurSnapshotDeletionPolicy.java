package org.apache.blur.manager.writer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.store.Directory;

public class BlurSnapshotDeletionPolicy extends SnapshotDeletionPolicy {

	public static final String SNAPSHOTS_PREFIX = "_snapshot";
	private Map<String, Long> mapOfSnapshotNameGen = new HashMap<String, Long>();

	private static final String SNAPSHOTS_TMPFILE_EXTENSION = ".tmp";
	Directory dir;
	
	public BlurSnapshotDeletionPolicy(IndexDeletionPolicy primary, Directory dir) throws IOException{
		super(primary);
		this.dir = dir;
		
		loadPriorSnapshots();
	}

	public BlurSnapshotDeletionPolicy(IndexDeletionPolicy indexDeletionPolicy) {
		super(indexDeletionPolicy);
	}

	public Map<String, Long> getAllSnapshots(){
		return mapOfSnapshotNameGen;
	}
	
	public IndexCommit getIndexCommitForSnapshotName(String snapshotFileName){
		Long gen = mapOfSnapshotNameGen.get(snapshotFileName);
		return getIndexCommit(gen);
	}
	
	public IndexCommit createSnapshot(String snapshotFileName) throws IOException{
		IndexCommit snapshot = snapshot();
		mapOfSnapshotNameGen.put(snapshotFileName, snapshot.getGeneration());
		return snapshot;
	}
	
	public void releaseGen(String snapshotName) throws IOException{
		releaseGen(mapOfSnapshotNameGen.get(snapshotName));
		mapOfSnapshotNameGen.remove(snapshotName);
	}
	
	/**
	 * Reads the snapshots information from the given {@link Directory}. This
	 * method can be used if the snapshots information is needed, however you
	 * cannot instantiate the deletion policy (because e.g., some other process
	 * keeps a lock on the snapshots directory).
	 */
	private synchronized void loadPriorSnapshots() throws IOException {
		long genLoaded = -1;
		for (String file : dir.listAll()) {
			if (file.endsWith(SNAPSHOTS_TMPFILE_EXTENSION)) {
				dir.deleteFile(file);
			} else {
				if (file.contains(SNAPSHOTS_PREFIX)) {
					long gen = Long.parseLong(file.substring(file.lastIndexOf("_") + 1, file.length()));
					String fileName = file.substring(0, file.lastIndexOf("_"));

					if (genLoaded == -1 || gen > genLoaded) {
						mapOfSnapshotNameGen.put(fileName, gen);
					
			          refCounts.clear();
			          refCounts.put(gen, 1);
					}
				}
			}
		}
	}
}