package org.apache.blur.agent.notifications;

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
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jivesoftware.smack.Chat;
import org.jivesoftware.smack.ChatManager;
import org.jivesoftware.smack.Connection;
import org.jivesoftware.smack.ConnectionConfiguration;
import org.jivesoftware.smack.MessageListener;
import org.jivesoftware.smack.XMPPConnection;
import org.jivesoftware.smack.XMPPException;
import org.jivesoftware.smack.packet.Message;

public class Messenger {
	private static final Log log = LogFactory.getLog(Messenger.class);
	
	private String host;
	private int port;
	private String username;
	private String password;
	private Set<String> recipients;
	private boolean sendMessage = false;
	
	public Messenger(Properties props) {
		if (props.containsKey("messenger.enabled") && props.getProperty("messenger.enabled").equals("true")) {
			sendMessage = true;
			host = props.getProperty("messenger.host");
			port = Integer.parseInt(props.getProperty("messenger.port", "5222"));
			username = props.getProperty("messenger.user");
			password = props.getProperty("messenger.password");
			
			recipients = new HashSet<String>();
			for (String recip : props.getProperty("messenger.recipients", "").split("\\|")) {
				if (StringUtils.isNotBlank(recip)) {
					recipients.add(recip);
				}
			}
		}
	}
	
	public void sendMessage(String message) {
		if (sendMessage && !recipients.isEmpty()) {
			try {
				// Create the configuration for this new connection
				ConnectionConfiguration config = new ConnectionConfiguration(host, port);
				config.setCompressionEnabled(true);
				config.setSASLAuthenticationEnabled(true);
				config.setSendPresence(false);
				config.setRosterLoadedAtLogin(false);
				
				Connection connection = new XMPPConnection(config);
				// Connect to the server
				connection.connect();
				// Log into the server
				connection.login(username, password);
				ChatManager manager = connection.getChatManager();
				for (String recipient : recipients) {
					Chat chat = manager.createChat(recipient, new MessageListener() {
						
						@Override
						public void processMessage(Chat chat, Message message) {
							try {
								chat.sendMessage("Sorry I'm just a bot");
							} catch (XMPPException e) {
								//Ignore response
							}
						}
					});
					chat.sendMessage(message);
				}
				// Disconnect from the server
				connection.disconnect();
			} catch (Exception e) {
				log.warn("Unable to send Instant Message: " + e.getMessage());
			}
		}
	}
}
