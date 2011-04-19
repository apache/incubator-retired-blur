package com.nearinfinity.blur.demo;

import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.nearinfinity.blur.thrift.BlurClientManager;
import com.nearinfinity.blur.thrift.commands.BlurCommand;
import com.nearinfinity.blur.thrift.generated.BlurQuery;
import com.nearinfinity.blur.thrift.generated.BlurResult;
import com.nearinfinity.blur.thrift.generated.BlurResults;
import com.nearinfinity.blur.thrift.generated.Column;
import com.nearinfinity.blur.thrift.generated.ColumnFamily;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.FetchRowResult;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.Selector;
import com.nearinfinity.blur.thrift.generated.Blur.Client;

public class DemoClient {

    public static void main(String[] args) throws Exception {
        BlurCommand<Void> command = new BlurCommand<Void>() {
            @Override
            public Void call(Client client) throws Exception {
                Random random = new Random();
                BlurQuery blurQuery = new BlurQuery();
                
                
                
                blurQuery.queryStr = "employee.name:zine";
                
                
                
                blurQuery.superQueryOn = true;
                blurQuery.uuid = random.nextLong();
                String table = "employee_super_mart";
                BlurResults results = client.query(table, blurQuery);
                System.out.println("totalResults=" + results.totalResults);
                for (BlurResult result : results.results) {
                    Selector selector = new Selector();
                    selector.setLocationId(result.locationId);
                    FetchResult fetchRow = client.fetchRow(table, selector);
                    FetchRowResult rowResult = fetchRow.rowResult;
                    Row row = rowResult.row;
                    System.out.println(row.id);
                    for (ColumnFamily columnFamily : row.columnFamilies) {
                        System.out.println("\t" + columnFamily.family);
                        Map<String, Set<Column>> records = columnFamily.records;
                        for (String recordId : records.keySet()) {
                            System.out.print("\t\t" + recordId);
                            for (Column column : records.get(recordId)) {
                                System.out.print(" " + column.name + ":" + column.values.get(0));
                            }
                            System.out.println();
                        }
                        
                    }
                }
                return null;
            }
        };
        BlurClientManager.execute("localhost:40020", command);

    }

}
