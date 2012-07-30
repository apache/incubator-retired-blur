<%@ page contentType="text/html; charset=UTF-8" isThreadSafe="false"
	import="javax.servlet.*" import="javax.servlet.http.*"
	import="java.io.*" import="java.util.*" import="java.text.DateFormat"
	import="java.lang.Math" import="java.net.URLEncoder"
	import="com.nearinfinity.blur.thrift.*"
	import="com.nearinfinity.blur.thrift.generated.*"
	import="com.nearinfinity.blur.thrift.generated.Blur.*"%>
<%@ include file="functions.jsp"%>
<%!
	public String shards(Iface client) throws Exception {
		String ret = "";
		List<String> servers = client.shardServerList("default");
		
		for(String s : servers) {
			ret += row(s,"","");
		}
		return ret;
	}
%>

<%
	//TODO: prop file the port
	String hostName = request.getServerName() + ":40010";

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
		<%=table(shards(client),"Shard") %>

	<%
		}
	%>
	<br/>
</body>
</html>