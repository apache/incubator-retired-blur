package com.nearinfinity.blur.demo;


public class DemoClient {

    public static void main(String[] args) throws Exception {
//        BlurCommand<Void> command = new BlurCommand<Void>() {
//            @Override
//            public Void call(Client client) throws Exception {
//                Random random = new Random();
//                BlurQuery blurQuery = new BlurQuery();
//                
//                
//                blurQuery.queryStr = "employee.name:john";
//                blurQuery.queryStr = "manager.salary:000000001*";
////                blurQuery.queryStr = "department.moreThanOneDepartment:T";
////                blurQuery.queryStr = "salary.makesMoreThanManager:T";
////                blurQuery.queryStr = "employee.birthDate:[1960-02-08 TO 1964-02-08]";
//                blurQuery.queryStr = "+title.title:\"Technique Leader\" +department.name:\"Overpowering Department\"";
////                blurQuery.queryStr = "+title.title:\"Senior Staff\" +salary.salary:[00000000100000 TO 00000000101000] +department.name:(\"Retreating Department\" \"Overpowering Department\")";
//                
//                
//                
//                blurQuery.fetch = 25;
//                blurQuery.superQueryOn = true;
//                blurQuery.uuid = random.nextLong();
//                String table = "employee_super_mart";
//                BlurResults results = client.query(table, blurQuery);
//                System.out.println("totalResults=" + results.totalResults);
//                for (BlurResult result : results.results) {
//                    Selector selector = new Selector();
//                    selector.setLocationId(result.locationId);
//                    FetchResult fetchRow = client.fetchRow(table, selector);
//                    FetchRowResult rowResult = fetchRow.rowResult;
//                    Row row = rowResult.row;
//                    System.out.println(row.id);
//                    for (ColumnFamily columnFamily : row.columnFamilies) {
//                        System.out.println("\t" + columnFamily.family);
//                        Map<String, Set<Column>> records = columnFamily.records;
//                        for (String recordId : records.keySet()) {
//                            System.out.print("\t\t" + recordId);
//                            for (Column column : records.get(recordId)) {
//                                System.out.print(" " + column.name + ":" + column.values.get(0));
//                            }
//                            System.out.println();
//                        }
//                        
//                    }
//                }
//                return null;
//            }
//        };
//        
//        for (int i = 0; i < 1000000; i++) {
//            long s = System.nanoTime();
//            BlurClientManager.execute("blur04.nearinfinity.com:40020", command);
//            long e = System.nanoTime();
//            System.out.println((e-s) / 1000000.0);
//        }

    }

}
