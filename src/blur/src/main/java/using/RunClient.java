package using;

import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.Hits;
import com.nearinfinity.blur.thrift.generated.ScoreType;
import com.nearinfinity.blur.thrift.generated.Blur.Client;

public class RunClient {
	
	public static void main(String... args) throws BlurException, TException {
		TTransport tr = new TSocket("localhost", 40020);
		TProtocol proto = new TBinaryProtocol(tr);
		Client client = new Client(proto);
		tr.open();
		
		List<String> tableList = client.tableList();
		System.out.println(tableList);
		long hitsTotal = 0;
		long totalTime = 0;
		long passes = 1;
		for (int i = 0; i < passes; i++) {
			long s = System.currentTimeMillis();
			Hits hits = client.search("test", "person.name:aaron", true, ScoreType.SUPER, null, 0, 10, Long.MAX_VALUE, Long.MAX_VALUE);
//			Hits hits = client.search("test", "test.test:value2", true, ScoreType.SUPER, null, 0, 0, Long.MAX_VALUE, Long.MAX_VALUE);
			hitsTotal += hits.totalHits;
			long e = System.currentTimeMillis();
			totalTime += (e-s);
		}
		
		System.out.println(hitsTotal + " " + (totalTime / (double) passes));
	}

}
