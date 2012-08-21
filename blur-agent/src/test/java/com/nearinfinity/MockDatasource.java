package com.nearinfinity;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

public class MockDatasource implements DataSource {

	@Override
	public PrintWriter getLogWriter() throws SQLException {
		throw new RuntimeException("Not yet implemented.");
	}

	@Override
	public int getLoginTimeout() throws SQLException {
		throw new RuntimeException("Not yet implemented.");
	}

	@Override
	public void setLogWriter(PrintWriter arg0) throws SQLException {
		throw new RuntimeException("Not yet implemented.");
	}

	@Override
	public void setLoginTimeout(int arg0) throws SQLException {
		throw new RuntimeException("Not yet implemented.");
	}

	@Override
	public boolean isWrapperFor(Class<?> arg0) throws SQLException {
		throw new RuntimeException("Not yet implemented.");
	}

	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException {
		throw new RuntimeException("Not yet implemented.");
	}

	@Override
	public Connection getConnection() throws SQLException {
		throw new RuntimeException("Not yet implemented.");
	}

	@Override
	public Connection getConnection(String arg0, String arg1) throws SQLException {
		throw new RuntimeException("Not yet implemented.");
	}

}
