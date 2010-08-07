package com.nearinfinity.blur.server;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import com.nearinfinity.blur.manager.SearchExecutor;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.blur.utils.HttpConstants;

public class BlurServer extends AbstractHandler implements HttpConstants,BlurConstants {
	
	//	private static final Log LOG = LogFactory.getLog(BlurServer.class);
	private static final String QUERY_IS_BLANK = "query is blank";

	public enum REQUEST_TYPE {
		STATUS,
		SEARCH,
		FAST_SEARCH,
		UNKNOWN
	}
	
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
				handleOther(target,baseRequest,request,response);
				return;
			}
		} catch (Exception e) {
			send(SC_INTERNAL_SERVER_ERROR,response,new Error().setError(e.getLocalizedMessage()));
		} finally {
			baseRequest.setHandled(true);
		}
	}

	protected void handleOther(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException {
		sendNotFoundError(target,response);
	}

	protected void send(int status, HttpServletResponse response, Object o) {
		response.setStatus(SC_INTERNAL_SERVER_ERROR);
		try {
			mapper.writeValue(response.getWriter(), o);
		} catch (JsonGenerationException e) {
			throw new RuntimeException(e);
		} catch (JsonMappingException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void sendNotFoundError(String target, HttpServletResponse response) throws IOException {
		send(SC_NOT_FOUND,response,new Error().setError("page not found").setPage(target));
	}

	private void handleFastSearch(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException {
		String table = getTable(target);
		String query = getQuery(request);
		String filter = getFilter(request);
		long minimum = getMinimum(request);

		if (query != null) {
			HitCount totalHits = new HitCount().setTotalHits(searchExecutor.searchFast(executor, table, query, filter, minimum));
			send(SC_OK, response, totalHits);
		} else {
			send(SC_INTERNAL_SERVER_ERROR, response, new Error().setError(QUERY_IS_BLANK));
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
			send(SC_OK, response, blurHits);
		} else {
			send(SC_INTERNAL_SERVER_ERROR, response, new Error().setError(QUERY_IS_BLANK));
		}
	}

	private long getStart(HttpServletRequest request) {
		String startStr = request.getParameter(SEARCH_START);
		if (startStr != null) {
			return Long.parseLong(startStr);
		}
		return 0;
	}

	private int getFetch(HttpServletRequest request) {
		String fetchStr = request.getParameter(SEARCH_FETCH_COUNT);
		if (fetchStr != null) {
			return Integer.parseInt(fetchStr);
		}
		return FETCH_DEFAULT;
	}

	private void handleStatus(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException {
		Map<String,Set<String>> map = new HashMap<String, Set<String>>();
		map.put("tables", searchExecutor.getTables());
		send(SC_OK, response, map);
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
			if (SEARCH_FAST.equals(split[2])) {
				return REQUEST_TYPE.FAST_SEARCH;
			}
		}
		return REQUEST_TYPE.UNKNOWN;
	}

	private long getMinimum(HttpServletRequest request) {
		String minStr = request.getParameter(SEARCH_MINIMUM);
		if (minStr == null) {
			return Long.MAX_VALUE;
		}
		return Long.parseLong(minStr);
	}

	private String getFilter(HttpServletRequest request) {
		return request.getParameter(SEARCH_FILTER);
	}

	private String getQuery(HttpServletRequest request) {
		return request.getParameter(SEARCH_QUERY);
	}

}
