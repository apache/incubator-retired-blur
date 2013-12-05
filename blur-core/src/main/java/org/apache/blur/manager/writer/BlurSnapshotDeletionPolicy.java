package org.apache.blur.manager.writer;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.store.Directory;

/**
 *	This class extends {@link SnapshotDeletionPolicy}. It loads the old snapshots
 *	and also contain a map of snapshot file name and corresponding generation.
 */
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