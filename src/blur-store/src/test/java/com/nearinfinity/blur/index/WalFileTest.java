package com.nearinfinity.blur.index;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Test;

import com.nearinfinity.blur.index.WalFile;

public class WalFileTest {

  @Test
  public void testTruncatedWalFileTest1() throws IOException {
    String name = "1.wal";
    RAMDirectory directory = new RAMDirectory();
    IndexOutput output = directory.createOutput(name);
    output.writeVInt(10);
    output.close();

    IndexInput input = directory.openInput(name);
    try {
      WalFile.readBlockOfDocs(input);
      fail("Should throw IOException");
    } catch (Exception e) {
      // pass
    }
  }

  @Test
  public void testTruncatedWalFileTest2() throws IOException {
    String name = "1.wal";
    RAMDirectory directory = new RAMDirectory();
    IndexOutput output = directory.createOutput(name);
    output.writeByte((byte) 0xFF);
    output.close();

    IndexInput input = directory.openInput(name);
    try {
      WalFile.readBlockOfDocs(input);
      fail("Should throw IOException");
    } catch (Exception e) {
      // pass
    }
  }

}
