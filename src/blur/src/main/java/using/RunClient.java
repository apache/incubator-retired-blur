package using;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.Blur.Client;

public class RunClient {
	
	public static void main(String... args) throws BlurException, TException {
		TTransport tr = new TSocket("localhost", 8080);
		TProtocol proto = new TBinaryProtocol(tr);
		Client client = new Client(proto);
		tr.open();
		
		long hitsTotal = 0;
		long totalTime = 0;
		long passes = 10000;
		for (int i = 0; i < passes; i++) {
			long s = System.currentTimeMillis();
			hitsTotal += client.countSearch("tablename", "test.test:value2", true, Long.MAX_VALUE);
			long e = System.currentTimeMillis();
			totalTime += (e-s);
		}
		
		System.out.println(hitsTotal + " " + (totalTime / (double) passes));
	}

}
