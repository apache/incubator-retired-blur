package org.apache.blur.thrift.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurResults;
import org.apache.blur.thrift.generated.Selector;
import org.apache.blur.thrift.generated.Query;

public class OverloadSystemv2 {

  public static void main(String[] args) throws InterruptedException, ExecutionException {

    ExecutorService service = Executors.newFixedThreadPool(100);
    final Iface client = BlurClient.getClient("blur-vm:40010");

    while (true) {
      List<Future<Void>> futures = new ArrayList<Future<Void>>();
      for (int i = 0; i < 100; i++) {
        futures.add(service.submit(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            BlurQuery blurQuery = new BlurQuery();
            Query query = new Query();
            query.setQuery("fam0.col0:*a* fam0.col0:*b* fam0.col0:*d* fam0.col0:*e* fam1.col0:*a* fam1.col0:*b* fam1.col0:*d* fam1.col0:*e*");
            blurQuery.setQuery(query);
            blurQuery.setUseCacheIfPresent(false);
            blurQuery.setCacheResult(false);
            blurQuery.setSelector(new Selector());
            blurQuery.setStart(0);
            blurQuery.setStart(100);
            try {
              BlurResults results = client.query("testtable", blurQuery);
              System.out.println(results.getTotalResults());
            } catch (BlurException e) {
              e.printStackTrace();
            } catch (TException e) {
              e.printStackTrace();
            }
            return null;
          }
        }));

      }
      for (Future<Void> future : futures) {
        future.get();
      }
    }
  }

}
