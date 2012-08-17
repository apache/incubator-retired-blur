<%@page import="java.text.DecimalFormat"%>
<%@page import="java.text.NumberFormat"%>
<%@ page contentType="text/html; charset=UTF-8" isThreadSafe="false"
	import="javax.servlet.*" import="javax.servlet.http.*"
	import="java.io.*" import="java.util.*" import="java.text.DateFormat"
	import="java.lang.Math" import="java.net.URLEncoder"
	import="com.nearinfinity.blur.thrift.*"
	import="com.nearinfinity.blur.thrift.generated.*"
	import="com.nearinfinity.blur.thrift.generated.Blur.*"%>
<%@ include file="functions.jsp"%>
<%!public boolean tableInSafeMode(Iface client, String clusterName) throws Exception {
		return client.isInSafeMode(clusterName);
	}

	public String getStats(Iface client, String tableName) throws Exception {
		DecimalFormat df = new DecimalFormat("#,###,###,###0.00");
		String ret = "";

		TableStats ts = client.getTableStats(tableName);
		String size = "";
		//bytes
		if(ts.bytes < 1000)
			size = ts.bytes + " bytes";
		//kb
		else if(ts.bytes < 1048576)
			size = df.format(ts.bytes/1000.0) + " KB";
		else if(ts.bytes < 1073741824)
			size = df.format(ts.bytes/1000.0/1000) + " MB";
		else if(ts.bytes < 137438953472l)
			size = df.format(ts.bytes/1000.0/1000/1000) + " GB";
		else if(ts.bytes < 1099511627776l)
			size = df.format(ts.bytes/1000.0/1000/1000/1000) + " TB";
		ret += row("size", size);
		ret += row("Queries", ts.queries + "");
		ret += row("Rows", ts.rowCount + "");
		ret += row("Records", ts.recordCount + "");
		
		return ret;
	}

	public String getSchema(Iface client, String tableName) throws Exception {
		String ret = "";

		Schema s = client.schema(tableName);
		for (String fam : s.columnFamilies.keySet()) {
			String tmp = "";
			for (String c : s.columnFamilies.get(fam))
				tmp += c + ", ";
			if (!"".equals(tmp))
				tmp = tmp.substring(0, tmp.length() - 2);
			ret += row(fam, tmp);
		}

		return ret;
	}

	public String getAD(Iface client, String tableName) throws Exception {
		String ret = "";

		TableDescriptor td = client.describe(tableName);
		AnalyzerDefinition ad = td.analyzerDefinition;
		Map<String, ColumnFamilyDefinition> cfds = ad.columnFamilyDefinitions;
		for (String cf : cfds.keySet()) {
			ColumnFamilyDefinition cfd = cfds.get(cf);
			if (cfd.defaultDefinition != null)
				ret += row(cf, "default",
						cfd.defaultDefinition.analyzerClassName);
			else
				ret += row(cf, "default", "none set");
			for (String col : cfd.columnDefinitions.keySet()) {
				ret += row("", col,
						cfd.columnDefinitions.get(col).analyzerClassName);
			}
		}

		return ret;
	}%>
<%
	final String NONE = "none given";

	String hostName = request.getServerName() + ":" + System.getProperty("blur.gui.servicing.port");

	Iface client = BlurClient.getClient(hostName);

	String tableName = request.getParameter("tableName");
	String clusterName = request.getParameter("clusterName");

	if (tableName == null || tableName.length() < 1) {
		tableName = NONE;
	}

	if (clusterName == null || clusterName.length() < 1) {
		clusterName = NONE;
	}
%>


<html>
<head>
<title>Table '<%=tableName%>'
</title>

<link href="style.css" rel="stylesheet" type="text/css" />

</head>

<body>
	<%
		if (NONE.equals(clusterName) || NONE.equals(tableName)) {
	%>
	Dont have a cluster and tableName specified, go home.
	<%
		} else {
	%>
	<h1>
		Table '<%=tableName%>'
	</h1>
	<%
		if (tableInSafeMode(client, clusterName)) {
	%>
		Cluster
		<%=clusterName%>
		is in safe mode, cannot retrieve table information yet.
	<%
			} else {
	%>
	
		<h2>Stats</h2>
		<%=table(getStats(client, tableName), "Stat",
								"Value")%>
		<br />
		<h2>Schema</h2>
		<%=table(getSchema(client, tableName),
								"ColumnFamily", "Column")%>
		<br />
		<h2>Field Definitions</h2>
		<%=table(getAD(client, tableName), "ColumnFamily",
								"Column", "Analyzer")%>
	
	<%
			}
		}
	%>

</body>
</html>

