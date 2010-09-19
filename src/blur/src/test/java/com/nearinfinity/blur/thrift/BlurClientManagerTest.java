package com.nearinfinity.blur.thrift;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.nearinfinity.blur.thrift.BlurClientManager.Command;
import com.nearinfinity.blur.thrift.generated.Blur.Client;
import static junit.framework.TestCase.*;

public class BlurClientManagerTest {
    
    
    
    private static final String CONNECTION_STR = "localhost:" + 7832;
    private static final int PORT = 7832;
    private Thread serverThread;
    protected BlurThriftServer thriftServer;

    @Test
    public void testBlurClientManager() throws Exception {
        startDoNothingServer();
        Thread.sleep(5000);
        List<String> list = BlurClientManager.execute(CONNECTION_STR, new Command<List<String>>() {
            @Override
            public List<String> call(Client client) throws Exception {
                return client.tableList();
            }
        });
        assertEquals(Arrays.asList("donothing"),list);
        stopDoNothingServer();
        Thread.sleep(5000);
        try {
            BlurClientManager.execute(CONNECTION_STR, new Command<List<String>>() {
                @Override
                public List<String> call(Client client) throws Exception {
                    return client.tableList();
                }
            });
            fail("The server should be down so this call should throw an exception");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void stopDoNothingServer() {
        thriftServer.stop();
        serverThread.interrupt();
        try {
            BlurClientManager.execute(CONNECTION_STR, new Command<List<String>>() {
                @Override
                public List<String> call(Client client) throws Exception {
                    return client.tableList();
                }
            });
        } catch (Exception e) {
            //do nothing, this is to release the server connections for this test.
        }
    }

    private void startDoNothingServer() {
        serverThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    thriftServer = new BlurThriftServer(PORT, new DoNothingServer());
                    thriftServer.start("NAME");
                } catch (Exception e) {
                    //do nothing
                }
                System.out.println("Server Thread is shutdown.");
            }
        });
        serverThread.setDaemon(true);
        serverThread.start();
    }

}
