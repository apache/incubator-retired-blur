package com.nearinfinity.blur.gui;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.codehaus.jackson.map.ObjectMapper;

import com.nearinfinity.blur.metrics.BlurMetrics;

public class MetricsServlet extends HttpServlet {
	
	private static final long serialVersionUID = 1L;

	private BlurMetrics bm = null;

	public MetricsServlet() {
	}

	
	public MetricsServlet(BlurMetrics bm) {
		this.bm = bm;
	}

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		response.setContentType("application/json");
		PrintWriter out = response.getWriter();
		ObjectMapper mapper = new ObjectMapper();
		mapper.writeValue(out, bm);
	}

}
