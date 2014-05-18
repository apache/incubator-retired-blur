/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.blur.console.util;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Locale;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletResponse;

import org.junit.Test;

public class HttpUtilTest {

	@Test
	public void testSendResponse() throws IOException {
		MockHttpServletResponse mockResponse = new MockHttpServletResponse();
		
		HttpUtil.sendResponse(mockResponse, "Test content", "text/plain");
		
		assertEquals("text/plain", mockResponse.getContentType());
		assertEquals(200, mockResponse.status);
		assertEquals("Test content".getBytes().length, mockResponse.contentLength);
		assertEquals("Test content", mockResponse.out.toString());
	}
	
	private class MockHttpServletResponse implements HttpServletResponse {
		public String contentType;
		public int contentLength;
		public int status;
		public ByteArrayOutputStream out;
		
		@Override
		public void setContentType(String type) {
			contentType = type;
		}
		
		@Override
		public void setContentLength(int len) {
			contentLength = len;
		}
		
		@Override
		public ServletOutputStream getOutputStream() throws IOException {
			out = new ByteArrayOutputStream();
			return new ServletOutputStream() {
				@Override
				public void write(int b) throws IOException {
					out.write(b);
				}
			};
		}
		
		@Override
		public String getContentType() {
			return contentType;
		}
		
		@Override
		public void setStatus(int sc) {
			status = sc;
		}
		
		@Override
		public void setLocale(Locale loc) {}
		@Override
		public void setCharacterEncoding(String charset) {}
		@Override
		public void setBufferSize(int size) {}
		@Override
		public void resetBuffer() {}
		@Override
		public void reset() {}
		@Override
		public boolean isCommitted() { return false; }
		@Override
		public PrintWriter getWriter() throws IOException { return null; }
		@Override
		public Locale getLocale() { return null; }
		@Override
		public String getCharacterEncoding() { return null; }
		@Override
		public int getBufferSize() { return 0; }
		@Override
		public void flushBuffer() throws IOException {}
		@Override
		public void setStatus(int sc, String sm) {}
        @Override
		public void setIntHeader(String name, int value) {}
		@Override
		public void setHeader(String name, String value) {}
		@Override
		public void setDateHeader(String name, long date) {}
		@Override
		public void sendRedirect(String location) throws IOException {}
		@Override
		public void sendError(int sc, String msg) throws IOException {}
		@Override
		public void sendError(int sc) throws IOException {}
		@Override
		public String encodeUrl(String url) { return null; }
		@Override
		public String encodeURL(String url) { return null; }
		@Override
		public String encodeRedirectUrl(String url) { return null; }
		@Override
		public String encodeRedirectURL(String url) { return null; }
		@Override
		public boolean containsHeader(String name) { return false; }
		@Override
		public void addIntHeader(String name, int value) {}
		@Override
		public void addHeader(String name, String value) {}
		@Override
		public void addDateHeader(String name, long date) {}
		@Override
		public void addCookie(Cookie cookie) {}
	}
}
