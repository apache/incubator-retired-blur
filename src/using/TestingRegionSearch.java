package using;

import com.nearinfinity.blur.hbase.BlurTable;

public class TestingRegionSearch {

	public static void main(String[] args) throws Exception {
		BlurTable table = new BlurTable("t1");
		while (true) {
			System.out.println(table.search("test.test:value", null, 0, 10));
			Thread.sleep(10);
		}
	}

}
