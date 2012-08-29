package com.nearinfinity.blur.testsuite;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.thrift.TException;

import com.nearinfinity.blur.thrift.BlurClient;
import com.nearinfinity.blur.thrift.generated.AnalyzerDefinition;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.BlurQuery;
import com.nearinfinity.blur.thrift.generated.BlurResults;
import com.nearinfinity.blur.thrift.generated.RowMutation;
import com.nearinfinity.blur.thrift.generated.RowMutationType;
import com.nearinfinity.blur.thrift.generated.SimpleQuery;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;

import static com.nearinfinity.blur.utils.BlurUtil.*;

/**
 * Tests alot of things, mainly connecting to a blur cluster and slamming a bunch
 * of rows in before querying for them.  I like to use it as a load test.
 * @author gman
 *
 */
public class CreateInsertQueryRepeating {

	private DecimalFormat df = new DecimalFormat("#,###,000.00");
	private static final char[] symbols = new char[36];

	static {
		for (int idx = 0; idx < 10; ++idx)
			symbols[idx] = (char) ('0' + idx);
		for (int idx = 10; idx < 36; ++idx)
			symbols[idx] = (char) ('a' + idx - 10);
	}

	private String table = "test1";
	private String host = "localhost";
	private Iface client = null;
	
	public CreateInsertQueryRepeating(String host, String table) throws BlurException, TException, IOException {
		this.host = host;
		this.table = table;
		
		//init
		String connectionStr = host + ":40010";
		String cluster = "default";
		client = BlurClient.getClient(connectionStr);
		
		List<String> clusterList = client.shardClusterList();
		if(clusterList != null && clusterList.size() > 0)
			cluster = clusterList.get(0);
		else
			throw new IOException("cannot find a cluster to use :(");
		
		System.out.println("using cluster: " + cluster);

		List<String> tableList = client.tableList();
		if (tableList == null || !tableList.contains(table))
			createTable(client, table, cluster);
		else
			System.out.println("table existed, did not create.");
	}
	
	private final Random random = new Random();

	public String randomString(int length) {
		char[] buf = new char[length];

		for (int idx = 0; idx < buf.length; ++idx)
			buf[idx] = symbols[random.nextInt(symbols.length)];
		return new String(buf);
	}

	public void getClusters(Iface client) {
		try {
			List<String> shardClusterList = client.shardClusterList();
			for (String cluster : shardClusterList)
				System.out.println("cluster: " + cluster);
		} catch (BlurException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
	}

	public void createTable(Iface client, String tableName, String cluster) throws BlurException, TException {
		TableDescriptor td = new TableDescriptor();
		td.analyzerDefinition = new AnalyzerDefinition();

		td.name = tableName;
		// TODO: doc doesnt say required, yet it barfs without it?
		td.cluster = cluster == null ? "default" : cluster;
		//auto enable table
		td.isEnabled = true;

		//1 shard per server :)
		td.shardCount = client.shardServerList(cluster).size();
		td.readOnly = false;
		// TODO: hardcodes bad, assuming NN on same node as BC
		td.tableUri = "hdfs://" + host + ":8020/" + tableName;
		client.createTable(td);
		System.out.println("table created");
	}

	/**
	 * @param args
	 * @throws TException
	 * @throws BlurException
	 * @throws IOException 
	 */
	public static void main(String[] args) throws BlurException, TException, IOException {
		String host = "localhost";
		String table = "test1";
		
		if(args != null) {
			if(args.length >= 1)
				host = args[0];
			if(args.length == 2)
				table = args[1];
		}
		
		CreateInsertQueryRepeating test = new CreateInsertQueryRepeating(host, table);

//		System.out.println("Testing joins real quick");
//		test.testJoin();
//		System.out.println("test done");
		
		System.out.println("Starting load");
		test.loadupTable(100);
		System.out.println("Finshed load");

		System.out.println("query time!");
		test.queryTable(50000);
		System.out.println("query done!");

		System.exit(0);
	}

	@SuppressWarnings("unused")
	private void testJoin() throws BlurException, TException {
		RowMutation mutation = new RowMutation();
		mutation.table = table;
		mutation.waitToBeVisible = true;
		mutation.rowId = "row1";
		mutation.addToRecordMutations(newRecordMutation("cf1",
				"recordid1", newColumn("col1","value1")));
		mutation.addToRecordMutations(newRecordMutation("cf1",
				"recordid2", newColumn("col2","value2")));
		mutation.rowMutationType = RowMutationType.REPLACE_ROW;
		client.mutate(mutation);
		
		List<String> joinTest = new ArrayList<String>();
		joinTest.add("+cf1.col1:value1");
		joinTest.add("+cf1.col2:value2");
		joinTest.add("+cf1.col1:value1 +cf1.col2:value2");
		joinTest.add("+(+cf1.col1:value1 nocf.nofield:somevalue) +(+cf1.col2.value2 nocf.nofield:somevalue)");
		joinTest.add("+(+cf1.col1:value1) +(cf1.bla:bla +cf1.col2.value2)");
		
		for(String q : joinTest)
			System.out.println(q + " hits: " + hits(client,table, q, true));
	}
	
	private static long hits(Iface client, String table, String queryStr, boolean superQuery) throws BlurException, TException {
		BlurQuery bq = new BlurQuery();
		SimpleQuery sq = new SimpleQuery();
		sq.queryStr = queryStr;
		sq.superQueryOn = superQuery;
		bq.simpleQuery = sq;
		BlurResults query = client.query(table, bq);
		return query.totalResults;
	}
	
	// really only useful against the table that was filled via loadupTable
	public void queryTable(int times) throws BlurException, TException {
		long start = System.currentTimeMillis();
		BlurQuery bq = new BlurQuery();
		bq.fetch = 10;
		for (int i = 1; i <= times; i++) {
			SimpleQuery sq = new SimpleQuery();
			sq.queryStr = "numberField:" + random.nextInt(1000);
			sq.superQueryOn = true;
			bq.simpleQuery = sq;
			client.query(table, bq);
			if (i % 1000 == 0) {
				System.out
						.println("queries: "
								+ i
								+ " times "
								+ df.format((i / ((System.currentTimeMillis()
										- start + 0.0)/1000))) + " queries/s");
			}
		}
		System.out
				.println("queries: "
						+ times
						+ " times "
						+ df.format((times / ((System.currentTimeMillis()
								- start + 0.0)/1000))) + " queries/s");

	}

	public void loadupTable(int rows) throws BlurException, TException, IOException {
		
		long start = System.currentTimeMillis();
		
		long buildTotal = 0;
		RowMutation mutation = new RowMutation();
		
		for (int i = 1; i <= rows; i++) {
			long buildStart = System.currentTimeMillis();
			mutation.clear();
			mutation.table = table;
			mutation.waitToBeVisible = false;
			mutation.rowId = UUID.randomUUID().toString();
			mutation.addToRecordMutations(newRecordMutation("test",
					"test-" + i,
					newColumn("uuidField", UUID.randomUUID().toString()),
					newColumn("numberField", i + ""),
					newColumn("fatTextField",randomString(1000))));
			mutation.rowMutationType = RowMutationType.REPLACE_ROW;

			if (i % 50 == 0) {
				System.out.println("loaded: " + i + " around "
						+ df.format((i / ((System.currentTimeMillis() - start+0.0)/1000) ))
						+ " rows/s");
				System.out.println("Total time: " + (System.currentTimeMillis()-start+0.0)/1000 +
						" Build time: "  + ((buildTotal/1000)+0.0) + " " + buildTotal);
			}			

			buildTotal += System.currentTimeMillis() - buildStart;

			client.mutate(mutation);

		}
		System.out.println("loaded: " + rows + " around "
				+ df.format((rows / ((System.currentTimeMillis() - start+0.0)/1000))) + " rows/s");
	}
}
