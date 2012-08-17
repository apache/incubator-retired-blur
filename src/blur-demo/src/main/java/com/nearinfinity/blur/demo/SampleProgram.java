package com.nearinfinity.blur.demo;

import static com.nearinfinity.blur.utils.BlurUtil.newColumn;
import static com.nearinfinity.blur.utils.BlurUtil.newRecordMutation;
import static com.nearinfinity.blur.utils.BlurUtil.newRowMutation;

import java.util.List;
import java.util.Random;

import org.apache.thrift.TException;

import com.nearinfinity.blur.thrift.BlurClient;
import com.nearinfinity.blur.thrift.generated.AnalyzerDefinition;
import com.nearinfinity.blur.thrift.generated.Blur;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.FetchRowResult;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.RowMutation;
import com.nearinfinity.blur.thrift.generated.Selector;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;

public class SampleProgram {
	
	private static final String BLUR_CLUSTER_NAME = "default";
	public static final String BLUR_CONTROLLER_HOSTNAME = "localhost";
	public static final String BLUR_CONTROLLER_PORT = "40010";
	
	public static final String HDFS_NAMENODE_HOSTNAME = "localhost";
	public static final String HDFS_NAMENODE_PORT = "9000";
	
	public static final String BLUR_TABLES_LOCATION = "/blur/tables/"; //Path must include trailing slash character
	private static final int  MAX_SAMPLE_ROWS = 1000;
	private static final int MAX_SEARCHES = 500;

	public static void main(String[] args) {
		try {
			//Connect
			Blur.Iface client = connect();
			
			//Delete all tables
			deleteAllTables(client);
			
			//Create a table
			String tableName = "SAMPLE_TABLE_" + System.currentTimeMillis();
			createTable(client, tableName);
			
			//List all the tables
			listTables(client);
			
			//Populate the table with data
			populateTable(client, tableName);
			
			//Run searches
			searchTable(client, tableName);	
			
			//Delete the table
			deleteTable(client, tableName);
		}
		catch (Exception e) {
			e.printStackTrace();
		}

	}


	private static void deleteAllTables(Iface client) throws BlurException, TException {
		List<String> tableList = listTables(client);
		for (String tableName: tableList) {
			deleteTable(client, tableName);
		}
	}


	private static Blur.Iface connect() {
		String blurConnectionString = BLUR_CONTROLLER_HOSTNAME + ":" + BLUR_CONTROLLER_PORT;
		System.out.println("Connecting to " + blurConnectionString);
		Blur.Iface client = BlurClient.getClient(blurConnectionString);
		System.out.println("Successfully connected to " + blurConnectionString);
		return client;
	}

	private static void createTable(Iface client, String tableName) {
		try {
			AnalyzerDefinition ad = new AnalyzerDefinition();

			TableDescriptor tableDescriptor = new TableDescriptor(); 
			tableDescriptor.setTableUri("hdfs://"+ HDFS_NAMENODE_HOSTNAME + ":" + HDFS_NAMENODE_PORT + BLUR_TABLES_LOCATION + tableName); 
			tableDescriptor.setAnalyzerDefinition(ad);
			tableDescriptor.setName(tableName);
			tableDescriptor.setCluster(BLUR_CLUSTER_NAME);

			System.out.println("About to create table " + tableName);
			client.createTable(tableDescriptor);
			System.out.println("Created table " + tableName);
		} catch (BlurException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
	}

	
	private static List<String> listTables(Blur.Iface client) {
		try {
			System.out.println("Listing all tables");
			List<String> tableNames = client.tableList();
			for (String tableName:tableNames) {
				System.out.println("tableName=" + tableName);
			}
			return tableNames;
		} catch (BlurException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	
	private static void populateTable(Iface client, String tableName) throws BlurException, TException {
		Random random = new Random();
		for (int count=1; count<= MAX_SAMPLE_ROWS; count++) {
			RowMutation mutation = newRowMutation(tableName, "rowid_" + count,
				newRecordMutation("sample", "recordid_1",
					newColumn("sampleData", "data_" + random.nextInt(50000))));
			System.out.println("About to add rowid_" + count);
			client.mutate(mutation);
			System.out.println("Added rowid_" + count);
		}
		
	}

	private static void searchTable(Iface client, String tableName) throws BlurException, TException {
		Random random = new Random();
		for (int count=1; count<=MAX_SEARCHES; count++) {
			String rowid = "rowid_" + random.nextInt(MAX_SAMPLE_ROWS);
			Selector selector = new Selector();
			selector.setRowId(rowid);
			FetchResult fetchRow = client.fetchRow(tableName, selector);
			if (fetchRow != null) {
				FetchRowResult rowResult = fetchRow.getRowResult();
				if (rowResult != null) {
					Row row = rowResult.getRow();
					if (row != null) {
						System.out.println("Found " + rowid);
					}

				}
			}
			else {
				System.out.println("Could not find " + rowid);
			}
		}
		
	}
	
	
	private static void deleteTable(Iface client, String tableName) throws BlurException, TException {
		client.disableTable(tableName);
		client.removeTable(tableName, true);		
		System.out.println("Deleted table " + tableName);
	}



}
