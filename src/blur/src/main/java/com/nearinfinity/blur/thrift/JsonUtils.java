package com.nearinfinity.blur.thrift;

import java.io.IOException;
import java.util.Arrays;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;

import com.nearinfinity.blur.thrift.generated.Column;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.SuperColumn;

public class JsonUtils {

	public static void main(String[] args) throws JsonGenerationException, JsonMappingException, IOException, TException {
		
		Row row = new Row();
		row.setId("rowid");

		SuperColumn sc = new SuperColumn("person", "superid", null);
		sc.addToColumns(new Column("name",Arrays.asList("val")));
		
		row.addToSuperColumns(sc);
		
		TMemoryBuffer trans = new TMemoryBuffer(10000);
		row.write(new TJSONProtocol(trans));
		System.out.println(trans.toString("UTF-8"));

	}

}
