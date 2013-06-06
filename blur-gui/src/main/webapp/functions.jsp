<%
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file 
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
%>
<%!public String row(String... col) {
		String ret = "<tr>";
		for (String c : col)
			ret += "<td>" + c + "</td>";
		return ret + "</tr>\n";
	}

	public String row(int colspan, String col) {
		return "<tr><td colspan=" + colspan + ">" + col + "</td></tr>";
	}

	public String shardListLink(String cluster, String display) {
		return "<a href='shardList.jsp?clusterName=" + cluster
				+ "' title='view shard servers'>" + display + "</a>";
	}
	
	public String table(String content, String... headers) {
		String ret = "<table class='statTable'><tr>\n";
		for(String h : headers)
			ret += "<td class='statTableTitle'>" + h + "</td>\n";
		return ret + "</tr>" + content + "</table>\n";
	}
	
%>