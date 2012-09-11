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
<%@page import="java.text.DecimalFormat"%>
<%@page import="java.text.NumberFormat"%>
<%@ page contentType="text/html; charset=UTF-8" isThreadSafe="false"
	import="javax.servlet.*" import="javax.servlet.http.*"
	import="java.io.*" import="java.util.*" import="java.text.DateFormat"
	import="java.lang.Math" import="java.net.URLEncoder"
	import="org.apache.blur.thrift.*"
	import="org.apache.blur.thrift.generated.*"
	import="org.apache.blur.thrift.generated.Blur.*"%>
<%@ include file="functions.jsp"%>
<%
	String hostName = request.getServerName() + ":" + System.getProperty("blur.gui.servicing.port");
%>
<html>
<head>
<title>Metrics</title>
<link href="style.css" rel="stylesheet" type="text/css" />
</head>
<script src="d3.v2.js"></script>
<body>

<script>
function draw(uri, id, data, margin, width, height, xLabel, yLabel, labels) {
  "use strict";
  var graphSvg = d3.select("#" + id).select("svg");
  if (graphSvg) {graphSvg.remove();}
  var graph = d3.select("#" + id).append("svg").attr("width", width).attr("height", height);

  var time_extent = d3.extent(data,function(d){return new Date(d.recordTime)});
  var time_scale = d3.time.scale().domain(time_extent).range([margin, width]);

  var maxArray = [];
  for (var index in labels) {
    var labelObject = labels[index];
	maxArray.push(d3.max(data,function(d){return d[labelObject.name];}));
  }
  var y_scale = d3.scale.linear().range([height-margin,margin]).domain([0, d3.max(maxArray)]);

  //add x axis
  var x_axis = d3.svg.axis().scale(time_scale);
  graph.append("g").attr("class","x axis " + id).attr("transform","translate(0,"+(height-margin)+")").call(x_axis);
  d3.select(".x.axis."+id).append("text").text(xLabel).attr("x", (width / 2) - margin).attr("y", margin / 1.5);

  //add y axis
  var y_axis = d3.svg.axis().scale(y_scale).orient("left");
  graph.append("g").attr("class","y axis " + id).attr("transform","translate(" + margin +",0)").call(y_axis);
  d3.select(".y.axis."+id).append("text").text(yLabel).attr("transform", "rotate (-90, 0, 0) translate(-" + (height/2) + ",-40)");

  //add lines
  for (var index in labels) {
    var labelObject = labels[index];
    var name = labelObject.name;
    var line = d3.svg.line().x(function(d){return time_scale(new Date(d.recordTime))}).y(function(d){return y_scale(d[name])}).interpolate("basis");
    var l = graph.append("path").attr("d", line(data)).attr("class", name + "_line");
    var s = labelObject.style;
	for (var v in s) {
      if (s.hasOwnProperty(v)) {
	    l.style(s,v[s]);
	  }
	}
  }
}

function drawGraphs(uri, margin, width, height) {
  d3.json(uri,function(data) {
	for (var graphId in data) {
      if (data.hasOwnProperty(graphId)) {
        draw(uri, graphId, data[graphId].lines.data, margin, width, height, data[graphId].xLabel, data[graphId].yLabel, data[graphId].lines.labels);
      }
	}
  });
  setTimeout(function() {
	drawGraphs(uri, margin, width, height);
  }, 1000);	
}

var margin = 50, width = 450, height = 220;

drawGraphs("/livemetrics", margin, width, height)

</script>
<h1>
	Blur <%=System.getProperty("blur.gui.mode") %> '<%=hostName%>'
</h1>
<h3>JVM Heap / Committed Heap</h3>
<div id="jvm"></div>
<h3>Blur Queries / Fetches / Mutates Requests</h3>
<div id="blur_calls"></div>
<h3>Blur Fetches / Mutates Record Rates</h3>
<div id="blur_recordRates"></div>
<h3>System Load</h3>
<div id="system"></div>
<%@ include file="footer.jsp" %>
</body>
</html>