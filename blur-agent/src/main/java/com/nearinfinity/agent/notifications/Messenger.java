package com.nearinfinity.agent.notifications;

import java.util.List;
import java.util.Properties;

public class Messenger {
	private String host;
	private int port;
	private String username;
	private String password;
	private List<String> recipients;
	private boolean sendMessage = false;
	
	public Messenger(Properties props) {
		if (props.containsKey("messenger.enabled") && props.getProperty("messenger.enabled").equals("true")) {
			host = props.getProperty("messenger.host");
			port = Integer.parseInt(props.getProperty("messenger.port", "5222"));
			username = props.getProperty("messenger.user");
			password = props.getProperty("messenger.password");
		}
	}
	
	public void sendMessage(String message) {
		
	}
}
