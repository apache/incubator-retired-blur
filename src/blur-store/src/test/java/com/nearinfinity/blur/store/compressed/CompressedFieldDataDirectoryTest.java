package com.nearinfinity.blur.store.compressed;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.junit.Test;

import com.nearinfinity.blur.store.DirectIODirectory;
import com.nearinfinity.blur.store.compressed.CompressedFieldDataDirectory;

public class CompressedFieldDataDirectoryTest {

  private static final CompressionCodec COMPRESSION_CODEC = CompressedFieldDataDirectory.DEFAULT_COMPRESSION;

  @Test
  public void testCompressedFieldDataDirectoryBasic() throws CorruptIndexException, IOException {
    RAMDirectory dir = new RAMDirectory();
    CompressedFieldDataDirectory directory = new CompressedFieldDataDirectory(DirectIODirectory.wrap(dir), COMPRESSION_CODEC);
    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_34, new KeywordAnalyzer());
    TieredMergePolicy mergePolicy = (TieredMergePolicy) config.getMergePolicy();
    mergePolicy.setUseCompoundFile(false);
    IndexWriter writer = new IndexWriter(directory, config);
    addDocs(writer, 0, 10);
    writer.close();
    testFetches(directory);
  }

  @Test
  public void testCompressedFieldDataDirectoryTransition() throws CorruptIndexException, LockObtainFailedException, IOException {
    RAMDirectory dir = new RAMDirectory();

    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_34, new KeywordAnalyzer());
    TieredMergePolicy mergePolicy = (TieredMergePolicy) config.getMergePolicy();
    mergePolicy.setUseCompoundFile(false);
    IndexWriter writer = new IndexWriter(dir, config);

    addDocs(writer, 0, 5);
    writer.close();

    CompressedFieldDataDirectory directory = new CompressedFieldDataDirectory(DirectIODirectory.wrap(dir), COMPRESSION_CODEC);
    config = new IndexWriterConfig(Version.LUCENE_34, new KeywordAnalyzer());
    mergePolicy = (TieredMergePolicy) config.getMergePolicy();
    mergePolicy.setUseCompoundFile(false);
    writer = new IndexWriter(directory, config);
    addDocs(writer, 5, 5);
    writer.close();
    testFetches(directory);
  }

  @Test
  public void testCompressedFieldDataDirectoryMixedBlockSize() throws CorruptIndexException, LockObtainFailedException, IOException {
    RAMDirectory dir = new RAMDirectory();
    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_34, new KeywordAnalyzer());
    TieredMergePolicy mergePolicy = (TieredMergePolicy) config.getMergePolicy();
    mergePolicy.setUseCompoundFile(false);
    IndexWriter writer = new IndexWriter(dir, config);
    addDocs(writer, 0, 5);
    writer.close();

    CompressedFieldDataDirectory directory1 = new CompressedFieldDataDirectory(DirectIODirectory.wrap(dir), COMPRESSION_CODEC, 2);
    config = new IndexWriterConfig(Version.LUCENE_34, new KeywordAnalyzer());
    mergePolicy = (TieredMergePolicy) config.getMergePolicy();
    mergePolicy.setUseCompoundFile(false);
    writer = new IndexWriter(directory1, config);
    addDocs(writer, 5, 2);
    writer.close();

    CompressedFieldDataDirectory directory2 = new CompressedFieldDataDirectory(DirectIODirectory.wrap(dir), COMPRESSION_CODEC, 4);
    config = new IndexWriterConfig(Version.LUCENE_34, new KeywordAnalyzer());
    mergePolicy = (TieredMergePolicy) config.getMergePolicy();
    mergePolicy.setUseCompoundFile(false);
    writer = new IndexWriter(directory2, config);
    addDocs(writer, 7, 3);
    writer.close();
    testFetches(directory2);
  }

  private void testFetches(Directory directory) throws CorruptIndexException, IOException {
    IndexReader reader = IndexReader.open(directory);
    for (int i = 0; i < reader.maxDoc(); i++) {
      String id = Integer.toString(i);
      Document document = reader.document(i);
      assertEquals(id, document.get("id"));
    }
  }

  private void addDocs(IndexWriter writer, int starting, int amount) throws CorruptIndexException, IOException {
    for (int i = 0; i < amount; i++) {
      int index = starting + i;
      writer.addDocument(getDoc(index));
    }
  }

  private Document getDoc(int index) {
    Document document = new Document();
    document.add(new Field("id", Integer.toString(index), Store.YES, Index.NOT_ANALYZED_NO_NORMS));
    return document;
  }

}
