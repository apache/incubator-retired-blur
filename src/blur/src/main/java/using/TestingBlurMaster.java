package using;

import com.nearinfinity.blur.thrift.BlurControllerServer;
import com.nearinfinity.blur.thrift.BlurShardServer;
import com.nearinfinity.blur.thrift.ThriftServer;
import com.nearinfinity.blur.utils.BlurConfiguration;


public class TestingBlurMaster {

	public static void main(String[] args) throws Exception {
		BlurConfiguration configuration = new BlurConfiguration();
		configuration.setInt("blur.shard.server.port",8081);
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					new ThriftServer(8080, new BlurControllerServer()).start();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}).start();
		
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					new ThriftServer(8081, new BlurShardServer()).start();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}).start();
	}

}
