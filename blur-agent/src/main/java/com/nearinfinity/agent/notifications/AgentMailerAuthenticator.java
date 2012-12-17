package com.nearinfinity.agent.notifications;

import javax.mail.Authenticator;
import javax.mail.PasswordAuthentication;

public class AgentMailerAuthenticator extends Authenticator {
	private final PasswordAuthentication authentication;

	public AgentMailerAuthenticator(String username, String password) {
		this.authentication = new PasswordAuthentication(username, password);
	}

	protected PasswordAuthentication getPasswordAuthentication() {
		return this.authentication;
	}
}
