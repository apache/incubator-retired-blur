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
<%
	String hostName = request.getServerName() + ":" + System.getProperty("blur.gui.servicing.port");
%>
<html>
<head>
<title>Metrics
</title>

<link href="style.css" rel="stylesheet" type="text/css" />

</head>

<script src="d3.v2.js"></script>
<body>
	<script>
		//decimal formatter
		var df = d3.format("4d");

		//basic printout
		d3.json("metrics", function(json) {
			//alert(json);
			arr = [];
			arr[0] = json;
			//select obj and bind data
			d3.select("div").selectAll("ul")
				.data(arr)
				.enter().append("ul")
				.text("Metrics")
				.selectAll("li")
				.data(function(d) {
					var map = [];
					var i = 0;
					for(var x in d) {
						var obj = {};
						obj.name = x;
						obj.value = d[x];
						map[i++] = obj;
					}
					return map})
					.enter()
					.append("li")
					.text(function(d) { return d.name + " " + d.value })
					.style("background-color", function(d,i) { return i % 2 ? "#eee" : "#ddd"; });
		});
	</script>
	<h1>
		Blur <%=System.getProperty("blur.gui.mode") %> '<%=hostName%>'
	</h1>
	<br />
<div>
</div>
<%@ include file="footer.jsp" %>
</body>
</html>

