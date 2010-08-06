package using;

import com.nearinfinity.blur.server.BlurMaster;
import com.nearinfinity.blur.server.BlurNode;

public class TestingBlurMaster {

	public static void main(String[] args) throws Exception {
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					BlurMaster.main(new String[]{"8080"});
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}).start();
		
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					BlurNode.main(new String[]{"8081"});
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}).start();
		
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					BlurNode.main(new String[]{"8082"});
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}).start();
	}

}
