package using;

import com.nearinfinity.blur.server.BlurMaster;
import com.nearinfinity.blur.server.BlurNode;

public class TestingBlurMaster {

	public static void main(String[] args) throws Exception {
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					new BlurMaster(8080).startServer();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}).start();
		
		Thread.sleep(10000);
		
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					new BlurNode(8081).startServer();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}).start();
		
		Thread.sleep(10000);
		
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					new BlurNode(8082).startServer();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}).start();
	}

}
