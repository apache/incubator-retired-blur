package com.nearinfinity.blur.testsuite;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import com.nearinfinity.blur.thrift.AsyncClientPool;
import com.nearinfinity.blur.thrift.BlurClient;
import com.nearinfinity.blur.thrift.generated.AnalyzerDefinition;
import com.nearinfinity.blur.thrift.generated.Blur;
import com.nearinfinity.blur.thrift.generated.Blur.AsyncClient.mutateBatch_call;
import com.nearinfinity.blur.thrift.generated.Blur.AsyncClient.mutate_call;
import com.nearinfinity.blur.thrift.generated.Blur.AsyncIface;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.BlurQuery;
import com.nearinfinity.blur.thrift.generated.ColumnDefinition;
import com.nearinfinity.blur.thrift.generated.ColumnFamilyDefinition;
import com.nearinfinity.blur.thrift.generated.RowMutation;
import com.nearinfinity.blur.thrift.generated.RowMutationType;
import com.nearinfinity.blur.thrift.generated.SimpleQuery;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;

import static com.nearinfinity.blur.utils.BlurUtil.*;

public class CreateInsertQueryRepeating {

	private static DecimalFormat df = new DecimalFormat("#,###,000.00");
	private static final char[] symbols = new char[36];

	static {
		for (int idx = 0; idx < 10; ++idx)
			symbols[idx] = (char) ('0' + idx);
		for (int idx = 10; idx < 36; ++idx)
			symbols[idx] = (char) ('a' + idx - 10);
	}

	private static final Random random = new Random();

	public static String randomString(int length) {
		char[] buf = new char[length];

		for (int idx = 0; idx < buf.length; ++idx)
			buf[idx] = symbols[random.nextInt(symbols.length)];
		return new String(buf);
	}

	public static void getClusters(Iface client) {
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

	public static void createTable(Iface client, String tableName,
			String cluster) throws BlurException, TException {
		TableDescriptor td = new TableDescriptor();
		td.analyzerDefinition = new AnalyzerDefinition();
		ColumnFamilyDefinition cfd = new ColumnFamilyDefinition();
		ColumnDefinition c = new ColumnDefinition(
				"org.apache.lucene.analysis.standard.StandardAnalyzer", true,
				null);
		cfd.putToColumnDefinitions("cf1", c);
		td.analyzerDefinition.putToColumnFamilyDefinitions("cf1", cfd);

		td.name = tableName;
		// TODO: doc doesnt say required, yet it barfs without it?
		td.cluster = cluster == null ? "default" : cluster;
		td.isEnabled = true;

		td.shardCount = 2;
		td.readOnly = false;
		// TODO: hardcodes bad
		td.tableUri = "hdfs://localhost:8020/" + tableName;
		client.createTable(td);
		// client.enableTable(tableName);
		System.out.println("table created");
	}

	/**
	 * @param args
	 * @throws TException
	 * @throws BlurException
	 * @throws IOException 
	 */
	public static void main(String[] args) throws BlurException, TException, IOException {
		String connectionStr = "localhost:40010";
		final String cluster = "default";
		String uri = "hdfs://localhost:8020/test1";
		int shardCount = 1;
		Iface client = BlurClient.getClient(connectionStr);

		List<String> tableList = client.tableList();
		if (tableList == null || !tableList.contains("test1"))
			createTable(client, "test1", cluster);

		System.out.println("Starting load");
		loadupTable(client, "test1", 1000);
		System.out.println("Finshed load");

		System.out.println("query time!");
		queryTable(client, "test1", 10000);
		System.out.println("query done!");

		System.out.println("going into auto create test: ");

		System.exit(0);
		int i = 0;
		while (i++ < 50) {
			String tableName = UUID.randomUUID().toString();
			System.out.println("Creating [" + tableName + "]");
			createTable(client, cluster, uri, shardCount, tableName);
			System.out.println("Loading [" + tableName + "]");
			loadTable(client, tableName);
			System.out.println("Disabling [" + tableName + "]");
			disable(client, tableName);
			System.out.println("Removing [" + tableName + "]");
			delete(client, tableName);
		}
	}

	// really only useful against the table that was filled via loadupTable
	private static void queryTable(Iface client, String tableName, int times)
			throws BlurException, TException {
		long start = System.currentTimeMillis();
		BlurQuery bq = new BlurQuery();
		bq.fetch = 10;
		for (int i = 1; i <= times; i++) {
			SimpleQuery sq = new SimpleQuery();
			sq.queryStr = "numberField:" + random.nextInt(1000);
			sq.superQueryOn = true;
			bq.simpleQuery = sq;
			client.query(tableName, bq);
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

	private static void loadupTable(Iface client, String tableName, int rows)
			throws BlurException, TException, IOException {
		AsyncClientPool pool = new AsyncClientPool();//10, 30000);
		AsyncIface poolClient = pool.getClient(Blur.AsyncIface.class, "localhost:40010");
		
		
		long start = System.currentTimeMillis();
		
		List<RowMutation> mutates = new ArrayList<RowMutation>();
		
		long buildTotal = 0;
		
		for (int i = 1; i <= rows; i++) {
			long buildStart = System.currentTimeMillis();
			RowMutation mutation = new RowMutation();
			mutation.table = tableName;
			mutation.waitToBeVisible = true;
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
//			mutates.add(mutation);

			poolClient.mutate(mutation, 
					new AsyncMethodCallback<Blur.AsyncClient.mutate_call>() {

						@Override
						public void onComplete(mutate_call response) {
						}

						@Override
						public void onError(Exception exception) {
							exception.printStackTrace();
						}
					});

//			if(mutates.size() == 10) {
//				pool.getClient(Blur.AsyncIface.class, "localhost:40010").mutateBatch(mutates,
//						new AsyncMethodCallback<Blur.AsyncClient.mutateBatch_call>() {
//						
//						@Override
//						public void onError(Exception exception) {	
//							exception.printStackTrace();
//						}
//						
//						@Override
//						public void onComplete(mutateBatch_call response) {
//						}
//					});
//				mutates.clear();
//			}
		}
		System.out.println("loaded: " + rows + " around "
				+ df.format((rows / ((System.currentTimeMillis() - start+0.0)/1000))) + " rows/s");
	}

	private static void disable(Iface client, String tableName)
			throws BlurException, TException {
		client.disableTable(tableName);
	}

	private static void delete(Iface client, String tableName)
			throws BlurException, TException {
		client.removeTable(tableName, true);
	}

	private static void loadTable(Iface client, String tableName)
			throws BlurException, TException {
		RowMutation mutation = new RowMutation();
		mutation.table = tableName;
		mutation.waitToBeVisible = true;
		mutation.rowId = "test";
		mutation.addToRecordMutations(newRecordMutation("test", "test",
				newColumn("test", "test")));
		mutation.rowMutationType = RowMutationType.REPLACE_ROW;
		client.mutate(mutation);
	}

	private static void createTable(Iface client, final String cluster,
			String uri, int shardCount, String tableName) throws BlurException,
			TException {
		final TableDescriptor tableDescriptor = new TableDescriptor();
		tableDescriptor.analyzerDefinition = new AnalyzerDefinition();
		tableDescriptor.cluster = cluster;

		tableDescriptor.name = tableName;
		tableDescriptor.readOnly = false;

		tableDescriptor.shardCount = shardCount;
		tableDescriptor.tableUri = uri + "/" + tableName;

		client.createTable(tableDescriptor);
	}

}
