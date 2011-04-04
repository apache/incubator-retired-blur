/*
 * Copyright (C) 2011 Near Infinity Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.blur.utils;

import static com.nearinfinity.blur.utils.BlurConstants.RECORD_ID;
import static com.nearinfinity.blur.utils.BlurConstants.ROW_ID;
import static com.nearinfinity.blur.utils.BlurConstants.SEP;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;

import com.nearinfinity.blur.thrift.generated.Column;
import com.nearinfinity.blur.thrift.generated.ColumnFamily;
import com.nearinfinity.blur.thrift.generated.FetchRecordResult;
import com.nearinfinity.blur.thrift.generated.Row;

public class RowDocumentUtil {
    
    public static FetchRecordResult getColumns(Document document) {
        FetchRecordResult result = new FetchRecordResult();
        Map<String, Column> columns = new HashMap<String, Column>();
        String columnFamily = null;
        for (Fieldable field : document.getFields()) {
            if (field.name().equals(ROW_ID)) {
                result.setRowid(field.stringValue());
            } else if (field.name().equals(RECORD_ID)) {
                result.setRecordid(field.stringValue());
            } else {
                String name = field.name();
                int index = name.indexOf('.');
                if (index < 0) {
                    continue;
                }
                if (columnFamily == null) {
                    columnFamily = name.substring(0,index);
                }
                name = name.substring(index + 1);
                String value = field.stringValue();
                Column column = columns.get(name);
                if (column == null) {
                    column = new Column();
                    column.setName(name);
                    columns.put(name, column);
                }
                column.addToValues(value);
            }
        }
        result.columnFamily = columnFamily;
        result.record = new HashSet<Column>(columns.values());
        return result;
    }

	public static Row getRow(Iterable<Document> docs) {
		Row row = new Row();
		boolean empty = true;
		if (docs == null) {
		    return null;
		}
		Map<String,ColumnFamily> columnFamilies = new HashMap<String, ColumnFamily>();
		for (Document document : docs) {
			empty = false;
			ColumnFamily newColumnFamily = convertToColumnFamily(row, document);
			if (newColumnFamily == null) {
			    continue;
			}
			String family = newColumnFamily.family;
			ColumnFamily columnFamily = columnFamilies.get(family);
			if (columnFamily == null) {
			    columnFamilies.put(family, newColumnFamily);
			} else {
			    columnFamily.records.putAll(newColumnFamily.records);
			}
		}
		if (empty) {
			return null;
		}
		row.columnFamilies = new TreeSet<ColumnFamily>(columnFamilies.values());
		return row;
	}

	public static ColumnFamily convertToColumnFamily(Row row, Document document) {
	    if (row.id == null) {
	        row.id = document.getField(ROW_ID).stringValue();
	    }
		String superColumnId = document.getField(RECORD_ID).stringValue();
		Map<String, Column> columns = new HashMap<String, Column>();
		String superColumnFamily = null;
		boolean empty = true;
		for (Fieldable fieldable : document.getFields()) {
			String name = fieldable.name();
			int index = name.indexOf(SEP);
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
			empty = false;
			column.addToValues(fieldable.stringValue());
		}
		if (empty) {
		    return null;
		}
		ColumnFamily columnFamily = new ColumnFamily().setFamily(superColumnFamily);
		Set<Column> columnSet = new TreeSet<Column>(BlurConstants.COLUMN_COMPARATOR);
		columnSet.addAll(columns.values());
        columnFamily.putToRecords(superColumnId, columnSet);
        return columnFamily;
	}
}
