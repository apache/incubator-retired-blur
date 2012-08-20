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
			
			var topLevelMetrics = d3.entries(json);
			
		    var columns = ["stat", "value"];

		    var table = d3.select("#leftTD").append("table"),
		        thead = table.append("thead"),
		        tbody = table.append("tbody");

		    // append the header row
		    thead.append("tr")
		        .selectAll("th")
		        .data(columns)
		        .enter()
		        .append("th")
		            .text(function(column) { return column; })
		            .style("text-align", function(d) { return "left"});
		    
		    // create a row for each object in the data
		    var rows = tbody.selectAll("tr")
		        .data(topLevelMetrics)
		        .enter()
		        .append("tr")
		        .style("background-color", function(d,i) { return i % 2 ? "#eee" : "#ddd"; });

		    // create a cell in each row for each column
		    var cells = rows.selectAll("td")
		        .data(function(row) {
		        	return d3.entries(row);
		        })
		        .enter()
		        .append("td")
		            .text(function(d) { return d.value; });


			
			
			//alert(json);
			arr = [];
			arr[0] = json;
			//select obj and bind data
			d3.select("#rightTD").selectAll("ul")
				.data(arr)
				.enter().append("ul")
				.text("Metrics")
				.selectAll("li")
				.data(function(d) {
					return d3.entries(d);})
					.enter()
					.append("li")
					.text(function(d) { return d.key + " " + d.value })
					.style("background-color", function(d,i) { return i % 2 ? "#eee" : "#ddd"; })
					.attr("id",function(d) { return d.key; });
		});
	</script>
	<h1>
		Blur <%=System.getProperty("blur.gui.mode") %> '<%=hostName%>'
	</h1>
	<br />
	<table>
	<tr>
	<td width="400" id="leftTD"></td>
	<td width="400" id="rightTD"></td>
	</tr>
	</table>
<div>
</div>
<%@ include file="footer.jsp" %>
</body>
</html>

