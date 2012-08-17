<%@ page contentType="text/html; charset=UTF-8" isThreadSafe="false"
	import="javax.servlet.*" import="javax.servlet.http.*"
	import="java.io.*" import="java.util.*" import="java.text.DateFormat"
	import="java.lang.Math" import="java.net.URLEncoder"
	import="com.nearinfinity.blur.thrift.*"
	import="com.nearinfinity.blur.thrift.generated.*"
	import="com.nearinfinity.blur.thrift.generated.Blur.*"%>
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
					ret += row(cluster, tableLink(table,cluster), client.describe(table).isEnabled?"yes":"no");
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
			ret += row(c, "Yes");
		}
		
		return ret;
	}%>
<%
	//TODO: prop file the port
	String hostName = request.getServerName() + ":" + System.getProperty("blur.gui.servicing.port");

	Iface client = BlurClient.getClient(hostName);
%>


<html>
<head>
<title>Blur Shard '<%=hostName%>'
</title>
<link href="style.css" rel="stylesheet" type="text/css" />
</head>
<body>
	<h1>
		Blur Shard '<%=hostName%>'
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
	<%=table(getTables(client),"Cluster Name","Table Name","Enabled")%>
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
	<br />
	<a href="logs">logs</a>
</body>

</html>
