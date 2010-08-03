package com.nearinfinity.blur.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;

import com.nearinfinity.blur.hbase.BlurHits;
import com.nearinfinity.blur.manager.DirectoryManagerImpl;
import com.nearinfinity.blur.manager.IndexManagerImpl;
import com.nearinfinity.blur.manager.SearchExecutorImpl;
import com.nearinfinity.blur.manager.SearchManagerImpl;
import com.nearinfinity.blur.manager.UpdatableManager;
import com.nearinfinity.blur.manager.dao.DirectoryManagerDao;

public class BlurRegion extends AbstractHandler implements HttpConstants {
	
	public enum REQUEST_TYPE {
		STATUS,
		SEARCH,
		FAST_SEARCH,
		UNKNOWN
	}
	
	private static final String FAST = "fast";
	private static final Log LOG = LogFactory.getLog(BlurRegion.class);
	private static final String QUERY = "q";
	private static final String FILTER = "f";
	private static final String MIME_TYPE = "text/html;charset=utf-8";
	private static final long TEN_SECONDS = 10000;
	private static final int FETCH_DEFAULT = 10;

	private DirectoryManagerImpl directoryManager;
	private IndexManagerImpl indexManager;
	private SearchManagerImpl searchManager;
	private SearchExecutorImpl searchExecutor;
	private Timer timer;

	private ExecutorService executor = Executors.newCachedThreadPool();

	public BlurRegion() {
		init();
	}

	private void init() {
		DirectoryManagerDao dao = new DirectoryManagerDao() {
			@Override
			public Map<String, Set<String>> getShardIdsToServe() {
				Map<String, Set<String>> shardIds = new TreeMap<String, Set<String>>();
				shardIds.put("test", new TreeSet<String>(Arrays.asList("test")));
				return shardIds;
			}
			@Override
			public URI getURIForShardId(String table, String shardId) {
				try {
					return new URI("file:///Users/amccurry/testIndex");
				} catch (URISyntaxException e) {
					throw new RuntimeException(e);
				}
			}
		};
		this.directoryManager = new DirectoryManagerImpl(dao);
		this.indexManager = new IndexManagerImpl(directoryManager);
		this.searchManager = new SearchManagerImpl(indexManager);
		this.searchExecutor = new SearchExecutorImpl(searchManager);
		update(directoryManager, indexManager, searchManager, searchExecutor);
		runUpdateTask(directoryManager, indexManager, searchManager, searchExecutor);
	}

	private void runUpdateTask(final UpdatableManager... managers) {
		TimerTask task = new TimerTask() {
			@Override
			public void run() {
				update(managers);
			}
		};
		this.timer = new Timer("Update-Manager-Timer", true);
		this.timer.schedule(task, TEN_SECONDS, TEN_SECONDS);
	}
	
	private void update(UpdatableManager... managers) {
		LOG.info("Running Update");
		for (UpdatableManager manager : managers) {
			manager.update();
		}
	}

	public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
		REQUEST_TYPE type = getRequestType(target);
		System.out.println("type " + target + " " + type);
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
			response.setContentType(MIME_TYPE);
			response.setStatus(SC_INTERNAL_SERVER_ERROR);
			PrintWriter printWriter = response.getWriter();
			printWriter.println("{\"error\":\""+e.getLocalizedMessage()+"\"}");
			printWriter.flush();
		} finally {
			baseRequest.setHandled(true);
		}
	}

	private void sendNotFoundError(String target, HttpServletResponse response) throws IOException {
		response.setContentType(MIME_TYPE);
		response.setStatus(SC_NOT_FOUND);
		PrintWriter printWriter = response.getWriter();
		printWriter.println("{\"error\":\"Page not found\", \"page\":\"" + target + "\"}");
		printWriter.flush();
	}

	private void handleFastSearch(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException {
		String table = getTable(target);
		long searchFast = -1;
		String query = getQuery(request);
		String filter = getFilter(request);
		long minimum = getMinimum(request);
		if (query != null) {
			searchFast = searchExecutor.searchFast(executor, table, query, filter, minimum);
		} else {
			response.setContentType(MIME_TYPE);
			response.setStatus(SC_INTERNAL_SERVER_ERROR);
			PrintWriter printWriter = response.getWriter();
			printWriter.println("{\"error\":\"query is blank\"}");
			printWriter.flush();
		}
		response.setContentType(MIME_TYPE);
		response.setStatus(SC_OK);
		PrintWriter printWriter = response.getWriter();
		printWriter.println("{\"totalhits\":"+searchFast+"}");
		printWriter.flush();
	}



	private void handleSearch(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException {
		String table = getTable(target);
		BlurHits blurHits = null;
		String query = getQuery(request);
		String filter = getFilter(request);
		long start = getStart(request);
		int fetch = getFetch(request);
		if (query != null) {
			blurHits = searchExecutor.search(executor, table, query, filter, start, fetch);
		} else {
			response.setContentType(MIME_TYPE);
			response.setStatus(SC_INTERNAL_SERVER_ERROR);
			PrintWriter printWriter = response.getWriter();
			printWriter.println("{\"error\":\"query is blank\"}");
			printWriter.flush();
		}
		response.setContentType(MIME_TYPE);
		response.setStatus(SC_OK);
		PrintWriter printWriter = response.getWriter();
		blurHits.toJson(printWriter);
		printWriter.flush();
	}

	private long getStart(HttpServletRequest request) {
		String startStr = request.getParameter("s");
		if (startStr != null) {
			return Long.parseLong(startStr);
		}
		return 0;
	}

	private int getFetch(HttpServletRequest request) {
		String fetchStr = request.getParameter("s");
		if (fetchStr != null) {
			return Integer.parseInt(fetchStr);
		}
		return FETCH_DEFAULT;
	}

	private void handleStatus(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException {
		response.setContentType(MIME_TYPE);
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

	public static void main(String[] args) throws Exception {
		Server server = new Server(8080);
		server.setHandler(new BlurRegion());
		server.start();
		server.join();
	}
}
