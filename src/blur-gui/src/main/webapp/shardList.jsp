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
<%@ include file="functions.jsp"%>
<%!
	public String shards(Iface client, String clusterName) throws Exception {
		String ret = "";
		List<String> servers = client.shardServerList(clusterName);
		
		for(String s : servers) {
			String[] split = s.split(":");
			int base = Integer.parseInt(System.getProperty("blur.base.shard.port"));
			int offset = Integer.parseInt(split[1])-base;
			int baseShardPort = Integer.parseInt(System.getProperty("baseGuiShardPort"));
			ret += row("<a href='http://" + split[0] + ":" + (baseShardPort + offset) + "'>" + s + "</a>","","");
			
		}
		return ret;
	}
%>

<%
	//TODO: prop file the port
	String hostName = request.getServerName() + ":" + System.getProperty("blur.gui.servicing.port");

	Iface client = BlurClient.getClient(hostName);

	String clusterName = request.getParameter("clusterName");
%>

<html>
<head>
<title>Blur Cluster Shard List '<%=hostName%>'
</title>
<link href="style.css" rel="stylesheet" type="text/css" />
</head>
<body>
	<%
		if (clusterName == null) {
	%>
	No cluster specified, go home.
	<%
		} else {
	%>
	<h1>
		Blur Shard List for Cluster '<%=clusterName%>'
	</h1>
	<br />
		<%=table(shards(client, clusterName),"Shard") %>

	<%
		}
	%>
<%@ include file="footer.jsp" %>
</body>
</html>