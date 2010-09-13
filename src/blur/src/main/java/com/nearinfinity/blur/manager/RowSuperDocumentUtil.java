package com.nearinfinity.blur.manager;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;

import com.nearinfinity.blur.lucene.index.SuperDocument;
import com.nearinfinity.blur.thrift.generated.Column;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.SuperColumn;
import com.nearinfinity.blur.utils.BlurConstants;

public class RowSuperDocumentUtil {

	public static Row getRow(String id, Iterable<Document> docs) {
		Row row = new Row();
		row.setId(id);
		boolean empty = true;
		for (Document document : docs) {
			empty = false;
			addDocumentToRow(row, document);
		}
		if (empty) {
			return null;
		}
		return row;
	}

	public static void addDocumentToRow(Row row, Document document) {
		String superColumnId = document.getField(SuperDocument.SUPER_KEY).stringValue();
		Map<String, Column> columns = new HashMap<String, Column>();
		String superColumnFamily = null;
		for (Fieldable fieldable : document.getFields()) {
			String name = fieldable.name();
			int index = name.indexOf(SuperDocument.SEP);
			if (index < 0) {
				//skip non super columns names.
				continue;
			}
			if (superColumnFamily == null) {
				superColumnFamily = name.substring(0,index);
			}
			Column column = columns.get(name);
			if (column == null) {
				column = new Column();
				column.name = name.substring(index+1);
				columns.put(name, column);
			}
			column.addToValues(fieldable.stringValue());
		}
		SuperColumn superColumn = new SuperColumn();
		superColumn.id = superColumnId;
		superColumn.family = superColumnFamily;
		superColumn.columns = new TreeSet<Column>(BlurConstants.COLUMN_COMPARATOR);
		superColumn.columns.addAll(columns.values());
		row.addToSuperColumns(superColumn);
	}
	
	public static SuperDocument createSuperDocument(Row row) {
		SuperDocument document = new SuperDocument(row.id);
		for (SuperColumn superColumn : row.superColumns) {
			for (Column column : superColumn.columns) {
				add(superColumn.family,superColumn.id,column,document);
			}
		}
		return document;
	}
	
	private static void add(String superColumnFamilyName, String superColumnId, Column column, SuperDocument document) {
		for (String value : column.values) {
			document.addFieldStoreAnalyzedNoNorms(superColumnFamilyName, superColumnId, column.name, value);
		}
	}
}
