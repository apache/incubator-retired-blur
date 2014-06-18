package org.apache.blur.console.model;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ResultRow {
	private String rowid;
	private List<Map<String, String>> records = new ArrayList<Map<String, String>>();

	public ResultRow() {

	}

	public ResultRow(String rowid) {
		this.rowid = rowid;
	}

	public String getRowid() {
		return rowid;
	}
	public void setRowid(String rowid) {
		this.rowid = rowid;
	}
	public List<Map<String, String>> getRecords() {
		return records;
	}
	public void setRecords(List<Map<String, String>> records) {
		this.records = records;
	}
}
