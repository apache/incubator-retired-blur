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

<%
	String hostName = request.getServerName() + ":" + System.getProperty("blur.gui.servicing.port");
%>
<html>
  <head>
    <title>Metrics</title>
    <link href="style.css" rel="stylesheet" type="text/css" />
  </head>
  <script src="js/jquery-1.9.1.min.js"></script>
  
  <script type="text/javascript">
    $(function() {
        $.getJSON('metrics', function(data) {
            $("#jvm").append(tree(data["jvm"]));
            
            var ignoreColumns = ["Operation", "Buffer Type", "Property"];
            var thriftCallsInfoColumns = ["Operation", "count", "max", "mean", "median", "min", "p75", "p95", "p98", "p99", "p999", "std_dev"];
            var thriftCallsTable = constructTable(data["org.apache.blur.Blur.Thrift Calls in µs"], thriftCallsInfoColumns, ignoreColumns);
            $("#thrift-calls").append(thriftCallsTable);
            
            var luceneInternalBuffersColumns = ["Buffer Type", "count", "m1", "m5", "m15", "mean", "type", "unit"];
            var thriftCallsTable = constructTable(data["org.apache.blur.Lucene.Internal Buffers"], luceneInternalBuffersColumns, ignoreColumns);
            $("#lucene-internal-buffers").append(thriftCallsTable);
            
            var blurDefaultColumns = ["Property", "type", "value"];
            var blurDefaultTable = constructTable(data["org.apache.blur.Blur.default"], blurDefaultColumns, ignoreColumns);
            $("#blur-default").append(blurDefaultTable);
            
            var blurLuceneTable = constructTable(data["org.apache.blur.Lucene"], blurDefaultColumns, ignoreColumns);
            $("#blur-lucene").append(blurLuceneTable);
            
            var blurColumns = ["Operation", "count", "m1", "m5", "m15", "mean", "type", "unit"];
            var blurTable = constructTable(data["org.apache.blur.Blur"], blurColumns, ignoreColumns);
            $("#blur").append(blurTable);
            
            $("#blur-fetchtimer").append(tree(data["org.apache.blur.Blur"]["Fetch Timer"]));
        }); 
    });
    
    /*
     * Builds and returns a UL/LI tree recirsively from the json data provided
     */
    function tree(data) {    
        if (typeof(data) == 'object') {        
            var ul = $('<ul>');
            for (var i in data) {            
                ul.append($('<li>').text(i).append(tree(data[i])));         
            }        
            return ul;
        } else {       
            var textNode = document.createTextNode(' : ' + data);
            return textNode;
        }
    }

    function constructTable(data, columns, ignoreColumns) {
    	var table = $("<table>");
		table.attr("border", "1px solid black");
		table.attr("border-collapse", "collapse");
		var thead = $("<thead>");
		var tbody = $("<tbody>");
		table.append(thead),
        table.append(tbody);
		
		var headrow = $("<tr>");
		for (var i in columns) {
			headrow.append($("<th>").text(columns[i]).css("text-align", "left").css("border", "1px solid black"));
		}
		thead.append(headrow);
	    
		$.each(data, function(k, v) {
			var ignoreKey = false;
		    var tr = $("<tr>");
		    tr.append($("<td>").text(camelCaseToNormal(k)).css("border", "1px solid black"));
		    for (var i in columns) {
		    	if ($.inArray(columns[i], ignoreColumns) == -1) {
		    		
		    		if (v[columns[i]] == undefined) {
		    			ignoreKey = true;
		    		} else {
		    			// fetch the value of the appropriate column
			    		var td = $("<td>");
		    		    td.text(v[columns[i]]);
		    		    td.css("border", "1px solid black");
		    	        tr.append(td);
		    		}
		    	}
		    }
		    
		    if (!ignoreKey) {
		        tbody.append(tr);
		    }
		});
		
		return table;
    }
    
    function camelCaseToNormal(text) {
    	return text.replace(/([A-Z])/g, ' $1').replace(/^./, function(str){ return str.toUpperCase(); })
    }

  </script>
  
  <body style="padding:20px;">
    <div id="header">
      <h1>Blur <%=System.getProperty("blur.gui.mode") %> '<%=hostName%>'</h1>
    </div>
	
	<div class="container">
      <div class="content">
        <div id="jvm"><h2>JVM Properties</h2></div>
      </div>
    
      <div class="content">
        <div id="thrift-calls"><h2>Thrift Calls</h2></div>
      </div>
    </div>
    
    <div class="container" style="clear:both;">
      <div id="blur" class="content" style="width:1300px;"><h2>Blur</h2></div>
    </div> 
    
    <div class="container" style="clear:both;">
      <div id="lucene-internal-buffers" class="content" style="width:1300px;"><h2>Lucene Internal Buffers</h2></div>
    </div>  

    <div class="container" style="clear:both;">
      <div id="blur-default" class="content" style="width:400px;"><h2>Blur Default</h2></div>
      <div id="blur-lucene"  class="content" style="width:400px;"><h2>Lucene</h2></div>
      <div id="blur-fetchtimer" class="content" style="width:300px;"><h2>Blur Fetch Timer</h2></div>
    </div>  
    
      <div id="footer">
        <%@ include file="footer.jsp" %>
      </div>
    
	
  </body>
</html>