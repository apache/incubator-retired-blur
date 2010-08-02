package com.nearinfinity.blur.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;

import com.nearinfinity.blur.manager.DirectoryManagerImpl;
import com.nearinfinity.blur.manager.IndexManagerImpl;
import com.nearinfinity.blur.manager.SearchExecutorImpl;
import com.nearinfinity.blur.manager.SearchManagerImpl;
import com.nearinfinity.blur.manager.UpdatableManager;
import com.nearinfinity.blur.manager.dao.DirectoryManagerDao;

public class BlurTable extends AbstractHandler {
	
	private static final String QUERY = "q";

	private static final String FILTER = "f";

	private static final String TEXT_HTML_CHARSET_UTF_8 = "text/html;charset=utf-8";

	private static final long TEN_SECONDS = 10000;

	private DirectoryManagerImpl directoryManager;
	private IndexManagerImpl indexManager;
	private SearchManagerImpl searchManager;
	private SearchExecutorImpl searchExecutor;
	private Timer timer;

	private ExecutorService executor = Executors.newCachedThreadPool();

	public BlurTable() {
		init();
	}

	private void init() {
		DirectoryManagerDao dao = new DirectoryManagerDao() {

			@Override
			public URI getURIForShardId(String shardId) {
				try {
					return new URI("file:///Users/amccurry/testIndex");
				} catch (URISyntaxException e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			public Set<String> getShardNamesToServe() {
				return new TreeSet<String>(Arrays.asList("test"));
			}
		};
		this.directoryManager = new DirectoryManagerImpl(dao);
		this.indexManager = new IndexManagerImpl(directoryManager);
		this.searchManager = new SearchManagerImpl(indexManager);
		this.searchExecutor = new SearchExecutorImpl(searchManager);
		updateTask(directoryManager, indexManager, searchManager, searchExecutor);
	}

	private void updateTask(final UpdatableManager... managers) {
		TimerTask task = new TimerTask() {
			@Override
			public void run() {
				System.out.println("Running update....");
				for (UpdatableManager manager : managers) {
					manager.update();
				}
			}
		};
		this.timer = new Timer("Update-Manager-Timer", true);
		this.timer.schedule(task, TEN_SECONDS, TEN_SECONDS);
	}

	public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
		
		baseRequest.setHandled(true);
		long searchFast = -1;
		try {
			String query = getQuery(request);
			String filter = getFilter(request);
			long minimum = getMinimum(request);
//			System.out.println("i got here [" + query + 
//					"] [" + filter +
//					"] [" + minimum + 
//					"]");
			if (query != null) {
				searchFast = searchExecutor.searchFast(executor, query, filter, minimum);
//				System.out.println(searchFast);
			} else {
				response.setContentType(TEXT_HTML_CHARSET_UTF_8);
				response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
				PrintWriter printWriter = response.getWriter();
				printWriter.println("{\"error\":\"query is blank\"}");
				printWriter.flush();
			}
			response.setContentType(TEXT_HTML_CHARSET_UTF_8);
			response.setStatus(HttpServletResponse.SC_OK);
			PrintWriter printWriter = response.getWriter();
			printWriter.println("{\"totalhits\":"+searchFast+"}");
			printWriter.flush();
		} catch (Exception e) {
			response.setContentType(TEXT_HTML_CHARSET_UTF_8);
			response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			PrintWriter printWriter = response.getWriter();
			printWriter.println("{\"error\":\""+e.getLocalizedMessage()+"\"}");
			printWriter.flush();
		}
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
		server.setHandler(new BlurTable());
		server.start();
		server.join();
	}
}
