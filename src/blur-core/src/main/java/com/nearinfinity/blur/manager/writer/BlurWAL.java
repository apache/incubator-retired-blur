package com.nearinfinity.blur.manager.writer;

import static com.nearinfinity.blur.utils.BlurUtil.newColumn;
import static com.nearinfinity.blur.utils.BlurUtil.newColumnFamily;
import static com.nearinfinity.blur.utils.BlurUtil.newRow;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransportException;

import com.nearinfinity.blur.thrift.generated.Row;

public class BlurWAL {
    
    private TIOStreamTransport _transport;
    private TBinaryProtocol _protocol;
    private Path _path;
    private Configuration _configuration;
    private FileSystem _fileSystem;
    private FSDataOutputStream _outputStream;
    private int size;
    private BufferedOutputStream _bufferedOutputStream;
    
    public static interface WalProcessor {
        void process(Row r);
    }
    
    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        Path path = new Path("hdfs://localhost:9000/blur.wal");
        for (int i = 0; i < 10; i++) {
            long s = System.nanoTime();
            BlurWAL.read(path,configuration,new WalProcessor() {
                private long l = 0;
                @Override
                public void process(Row r) {
                    l+=r.hashCode();
                }
            });
            long e = System.nanoTime();
            System.out.println((e-s) / 1000000.0);
        }
        
        BlurWAL wal = new BlurWAL();
        wal.setConfiguration(configuration);
        wal.setPath(path);
        wal.init();
        for (int i = 0; i < 1000; i++) {
            wal.append(genRow());
        }
    }

    public void setConfiguration(Configuration configuration) {
        _configuration = configuration;        
    }

    public void setPath(Path path) {
        _path = path;
    }

    private static Collection<Row> genRow() {
        Row row = newRow(UUID.randomUUID().toString(), 
                newColumnFamily("test-family", UUID.randomUUID().toString(), 
                        newColumn("testcol1", UUID.randomUUID().toString()),
                        newColumn("testcol2", UUID.randomUUID().toString()), 
                        newColumn("testcol3", UUID.randomUUID().toString())));
        return Arrays.asList(row);
    }
    
    public void close() {
        _transport.close();
    }

    public static void read(Path path, Configuration configuration, WalProcessor walProcessor) throws IOException {
        FileSystem fileSystem = FileSystem.get(path.toUri(), configuration);
        if (!fileSystem.exists(path)) {
            return;
        }
        FSDataInputStream inputStream = fileSystem.open(path);
        TIOStreamTransport trans = new TIOStreamTransport(inputStream);
        TBinaryProtocol proto = new TBinaryProtocol(trans);
        Row row = new Row();
        while (true) {
            try {
                row.read(proto);
            } catch (TTransportException e) {
                if (e.getType() == TTransportException.END_OF_FILE) {
                    trans.close();
                    return;
                }
            } catch (TException e) {
                throw new IOException(e);
            }
            walProcessor.process(row);
        }
    }

    public void init() throws IOException {
        _fileSystem = FileSystem.get(_path.toUri(), _configuration);
        _outputStream = _fileSystem.create(_path,true);
        _bufferedOutputStream = new BufferedOutputStream(_outputStream);
        _transport = new TIOStreamTransport(_bufferedOutputStream);
        _protocol = new TBinaryProtocol(_transport);
    }
    
    public synchronized void append(Iterable<Row> rows) throws IOException {
        for (Row row : rows) {
            try {
                row.write(_protocol);
            } catch (TException e) {
                throw new IOException(e);
            }
        }
        _bufferedOutputStream.flush();
        _outputStream.sync();
    }
}
