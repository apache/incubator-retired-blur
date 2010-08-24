package using;

import java.io.IOException;

import org.apache.thrift.TException;

import com.nearinfinity.blur.thrift.BlurShardServer;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.Hits;
import com.nearinfinity.blur.thrift.generated.ScoreType;

public class TestingShard {

	public static void main(String[] args) throws IOException, BlurException, TException {
		BlurShardServer server = new BlurShardServer();
		Hits hits = server.search("test", "person.name", true, ScoreType.SUPER, null, 0, 10, Long.MAX_VALUE, Long.MAX_VALUE);
		System.out.println(hits);
	}

}
