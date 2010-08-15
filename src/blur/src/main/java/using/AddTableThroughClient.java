package using;

import java.net.URISyntaxException;
import java.util.HashMap;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.thrift.generated.Blur.Client;

public class AddTableThroughClient {
	
	public static void main(String... args) throws BlurException, TException, URISyntaxException {
		TTransport tr = new TSocket("localhost", 8081);
		TProtocol proto = new TBinaryProtocol(tr);
		Client client = new Client(proto);
		tr.open();
		
//		TableDescriptor desc = new TableDescriptor();
//		desc.analyzerDef = "";
//		desc.shardDirectoryLocations = new HashMap<String, String>();
//		desc.shardDirectoryLocations.put("shard1", "file:///Users/amccurry/testIndex");
//		desc.shardDirectoryLocations.put("shard2", "file:///Users/amccurry/testIndex");
//		desc.shardDirectoryLocations.put("shard3", "file:///Users/amccurry/testIndex");
//		desc.shardDirectoryLocations.put("shard4", "file:///Users/amccurry/testIndex");
//		client.create("test", desc);
		
//		client.enable("test");
		
		
		//need to make sure readers are closed....
		client.disable("test");
	}

}
