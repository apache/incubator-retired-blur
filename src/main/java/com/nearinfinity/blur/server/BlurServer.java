package com.nearinfinity.blur.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.codehaus.jackson.map.ObjectMapper;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import com.nearinfinity.blur.manager.SearchExecutor;

public class BlurServer extends AbstractHandler implements HttpConstants {
//	private static final Log LOG = LogFactory.getLog(BlurServer.class);
	private static final String QUERY_IS_BLANK = "query is blank";

	public enum REQUEST_TYPE {
		STATUS,
		SEARCH,
		FAST_SEARCH,
		UNKNOWN
	}
	
	private static final String FAST = "fast";
	private static final String QUERY = "q";
	private static final String FILTER = "f";
	private static final String MIME_TYPE = "text/html;charset=utf-8";
	
	private static final int FETCH_DEFAULT = 10;

	protected SearchExecutor searchExecutor;

	private ExecutorService executor = Executors.newCachedThreadPool();
	private ObjectMapper mapper = new ObjectMapper();

	public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
		response.setContentType(MIME_TYPE);
		REQUEST_TYPE type = getRequestType(target);
		try {
			switch (type) {
			case STATUS:
				handleStatus(target,baseRequest,request,response);
				return;
			case SEARCH:
				handleSearch(target,baseRequest,request,response);
				return;
			case FAST_SEARCH:
				handleFastSearch(target,baseRequest,request,response);
				return;
			default:
				sendNotFoundError(target,response);
				return;
			}
		} catch (Exception e) {
			response.setStatus(SC_INTERNAL_SERVER_ERROR);
			PrintWriter printWriter = response.getWriter();
			printWriter.println("{\"error\":\""+e.getLocalizedMessage()+"\"}");
			printWriter.flush();
		} finally {
			baseRequest.setHandled(true);
		}
	}

	private void sendNotFoundError(String target, HttpServletResponse response) throws IOException {
		response.setStatus(SC_NOT_FOUND);
		mapper.writeValue(response.getWriter(), new Error().setError("page not found").setPage(target));
	}

	private void handleFastSearch(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException {
		String table = getTable(target);
		String query = getQuery(request);
		String filter = getFilter(request);
		long minimum = getMinimum(request);

		if (query != null) {
			HitCount totalHits = new HitCount().setTotalHits(searchExecutor.searchFast(executor, table, query, filter, minimum));
			response.setStatus(SC_OK);
			mapper.writeValue(response.getWriter(), totalHits);
		} else {
			response.setStatus(SC_INTERNAL_SERVER_ERROR);
			mapper.writeValue(response.getWriter(), new Error().setError(QUERY_IS_BLANK));
		}
	}
	private void handleSearch(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException {
		String table = getTable(target);
		String query = getQuery(request);
		String filter = getFilter(request);
		long start = getStart(request);
		int fetch = getFetch(request);
		
		if (query != null) {
			BlurHits blurHits = searchExecutor.search(executor, table, query, filter, start, fetch);
			response.setStatus(SC_OK);
			mapper.writeValue(response.getWriter(), blurHits);
		} else {
			response.setStatus(SC_INTERNAL_SERVER_ERROR);
			mapper.writeValue(response.getWriter(), new Error().setError(QUERY_IS_BLANK));
		}
	}

	private long getStart(HttpServletRequest request) {
		String startStr = request.getParameter("s");
		if (startStr != null) {
			return Long.parseLong(startStr);
		}
		return 0;
	}

	private int getFetch(HttpServletRequest request) {
		String fetchStr = request.getParameter("c");
		if (fetchStr != null) {
			return Integer.parseInt(fetchStr);
		}
		return FETCH_DEFAULT;
	}

	private void handleStatus(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException {
		response.setStatus(SC_OK);
		PrintWriter printWriter = response.getWriter();
		Set<String> tables = searchExecutor.getTables();
		boolean flag = true;
		printWriter.println("{\"tables\":[");
		for (String table : tables) {
			if (!flag) {
				printWriter.print(',');
			}
			printWriter.print('"');
			printWriter.println(table);
			printWriter.print('"');
			flag = false;
		}
		printWriter.println("]}");
		printWriter.flush();
	}
	
	private String getTable(String target) {
		String[] split = target.split("/");
		if (split.length >= 2) {
			return split[1];
		}
		throw new IllegalArgumentException("Target does not contain a table name [" + target + "]");
	}

	private REQUEST_TYPE getRequestType(String target) {
		String[] split = target.split("/");
		if (split.length == 0) {
			return REQUEST_TYPE.STATUS;
		} else if (split.length == 1) {
			return REQUEST_TYPE.STATUS;
		} else if (split.length == 2) {
			return REQUEST_TYPE.SEARCH;
		} else if (split.length == 3) {
			if (FAST.equals(split[2])) {
				return REQUEST_TYPE.FAST_SEARCH;
			}
		}
		return REQUEST_TYPE.UNKNOWN;
	}

	private long getMinimum(HttpServletRequest request) {
		String minStr = request.getParameter("min");
		if (minStr == null) {
			return Long.MAX_VALUE;
		}
		return Long.parseLong(minStr);
	}

	private String getFilter(HttpServletRequest request) {
		return request.getParameter(FILTER);
	}

	private String getQuery(HttpServletRequest request) {
		return request.getParameter(QUERY);
	}

}
