package com.nearinfinity.blur.lucene.store;

import static com.nearinfinity.blur.utils.BlurUtil.newColumn;
import static com.nearinfinity.blur.utils.BlurUtil.newColumnFamily;
import static com.nearinfinity.blur.utils.BlurUtil.newRow;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.utils.RowIndexWriter;

public class CreateSampleBlurIndex {

    public static void main(String[] args) throws IOException {
        FileSystem fileSystem = FileSystem.get(new Configuration());
        Path hdfsDirPath = new Path("/Users/amccurry/Development/blur/blur/trunk/src/blur-core/local-testing/table/shard3");
        HdfsDirectory hdfsDir = new HdfsDirectory(hdfsDirPath, fileSystem);

        BlurAnalyzer analyzer = new BlurAnalyzer(new StandardAnalyzer(Version.LUCENE_30), "");
        FSDirectory directory = FSDirectory.open(new File("./tmp/testing123"));
        IndexWriter writer = new IndexWriter(directory, analyzer, MaxFieldLength.UNLIMITED);
        RowIndexWriter indexWriter = new RowIndexWriter(writer, analyzer);
        Row row = newRow("1", newColumnFamily("cf1", "2", newColumn("col1", "val1")));
        indexWriter.replace(row);
        writer.close();
        
        Directory.copy(directory, hdfsDir, true);
        
//        String s = "{\"default\":\"org.apache.lucene.analysis.standard.StandardAnalyzer\"};";
//        Path analyzerPath = new Path("/Users/amccurry/Development/blur/blur/trunk/src/blur-core/local-testing/table/analyzer.json");
//        FSDataOutputStream outputStream = fileSystem.create(analyzerPath);
//        outputStream.write(s.getBytes());
//        outputStream.close();
        
//        Path enabled = new Path("/Users/amccurry/Development/blur/blur/trunk/src/blur-core/local-testing/table/enabled");
//        fileSystem.create(enabled).close();
        
        
    }

}
