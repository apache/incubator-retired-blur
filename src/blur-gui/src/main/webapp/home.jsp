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
<%@ page contentType="text/html; charset=UTF-8" isThreadSafe="false"
	import="javax.servlet.*" import="javax.servlet.http.*"
	import="java.io.*" import="java.util.*" import="java.text.DateFormat"
	import="java.lang.Math" import="java.net.URLEncoder"
	import="org.apache.blur.thrift.*"
	import="org.apache.blur.thrift.generated.*"
	import="org.apache.blur.thrift.generated.Blur.*"%>
<%@ include file="functions.jsp" %>
<%!

	public String tableLink(String tableName, String clusterName) {
		return "<a href='table.jsp?tableName="+tableName+"&clusterName="+clusterName+"' title='view details for " + tableName + "'>" + tableName + "</a>";
	}
	
	public String getTables(Iface client) throws Exception {
		String ret = "";
		List<String> clusters = client.shardClusterList();
		for (String cluster : clusters) {
			//tables: _ tableName : enabled
			List<String> tables = client.tableListByCluster(cluster);
			for (String table : tables) {
				try {
					TableDescriptor td = client.describe(table);
					ret += row(cluster, tableLink(table,cluster), td.shardCount+"", td.isEnabled?"yes":"no");
				} catch (BlurException e) {
					ret += row(3, "<font color=FF0000>Error describing table: "
							+ table + "</font>");
				}
			}
		}
		return ret;
	}

	public String getClusters(Iface client) throws Exception {
		String ret = "";
		List<String> clusters = client.shardClusterList();
		for (String cluster : clusters) {
			ret += row(cluster, shardListLink(cluster,client.shardServerList(cluster).size()+""), getClusterEnabled(client, cluster));
		}
		return ret;
	}

	
	public String getConf(Iface client) throws Exception {
		return row(2,"disabled as its broken");
/*		try {
			Map<String, String> config = client.configuration();
			String ret = "";
			for (String key : config.keySet()) {
				ret += row(key, config.get(key));
			}
			return ret;
		} catch (Exception e) {
			return row(2, "Cannot retrieve anything from client.configuration.");
		}*/
	}

	public String getClusterEnabled(Iface client, String cluster)
			throws Exception {
		return client.isInSafeMode(cluster) ? "Safe Mode On"
				: "Yes";
	}

	public String getControllers(Iface client) throws Exception {
		String ret = "";
		List<String> con = client.controllerServerList();

		for (String c : con) {
			String[] split = c.split(":");
			int base = Integer.parseInt(System.getProperty("blur.base.controller.port"));
			int offset = Integer.parseInt(split[1])-base;
			int baseShardPort = Integer.parseInt(System.getProperty("baseGuiControllerPort"));
			ret += row("<a href='http://" + split[0] + ":" + (baseShardPort + offset) + "'>" + c + "</a>","Yes");
		}
		
		return ret;
	}%>
<%
	String hostName = request.getServerName() + ":" + System.getProperty("blur.gui.servicing.port");

	Iface client = BlurClient.getClient(hostName);
%>


<html>
<head>
<title>Blur <%=System.getProperty("blur.gui.mode") %> '<%=hostName%>'
</title>
<link href="style.css" rel="stylesheet" type="text/css" />
</head>
<body>
	<h1>
		Blur <%=System.getProperty("blur.gui.mode") %> '<%=hostName%>'
	</h1>
	<br />
	<h2>Controllers</h2>
	<%=table(getControllers(client),"Name","Online") %>
	<hr />
	<br />
	<h2>Clusters</h2>
	<%=table(getClusters(client),"Cluster Name","Shard Servers","Enabled") %>
	<hr />
	<br />
	<h2>Tables</h2>
	<%=table(getTables(client),"Cluster Name","Table Name","Shards","Enabled")%>
	<hr />
	<br />
	<h2>Configs</h2>
	<table class="statTable" class="statTableTitle">
		<tr>
			<td class="statTableTitle">Param</td>
			<td class="statTableTitle">Value</td>
		</tr>
		<%=getConf(client)%>
	</table>
	<hr />
<%@ include file="footer.jsp" %>
</body>

</html>
