import java.io.IOException;

import com.nearinfinity.blur.thrift.BlurThriftServer;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.utils.BlurConstants;


public class BlurControllerThriftServer {
    public static void main(String[] args) throws IOException, BlurException, InterruptedException {
        BlurThriftServer.main(BlurConstants.CONTROLLER);
    }
}
