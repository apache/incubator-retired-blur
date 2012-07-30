
<%!public String row(String... col) {
		String ret = "<tr>";
		for (String c : col)
			ret += "<td>" + c + "</td>";
		return ret + "</tr>\n";
	}

	public String row(int colspan, String col) {
		return "<tr><td colspan=" + colspan + ">" + col + "</td></tr>";
	}

	public String shardListLink(String cluster, String display) {
		return "<a href='shardList.jsp?clusterName=" + cluster
				+ "' title='view shard servers'>" + display + "</a>";
	}
	
	public String table(String content, String... headers) {
		String ret = "<table class='statTable'><tr>\n";
		for(String h : headers)
			ret += "<td class='statTableTitle'>" + h + "</td>\n";
		return ret + "</tr>" + content + "</table>\n";
	}
	
	%>