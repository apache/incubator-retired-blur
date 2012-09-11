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
<!DOCTYPE html>
<meta charset="utf-8"></meta>
<style>
.axis path {
  fill:none;
  stroke: black;
}

.tick {
  fill:none;
  stroke:black;
} 

.axis {
  font-family: arial;
  font-size:0.6em;
}

path {
  fill:none;
  stroke:black;
  stroke-width:2px;
}
</style>
<body>
<script src="d3.v2.js"></script>
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

var margin = 50, width = 600, height = 250;

drawGraphs("/livemetrics", margin, width, height)

</script>

<div id="jvm"></div>
<div id="blur"></div>
<div id="system"></div>

</body>