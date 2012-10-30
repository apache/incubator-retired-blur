package com.nearinfinity.agent.mailer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AgentMailer {
	private static final Log log = LogFactory.getLog(AgentMailer.class);

	private static AgentMailer mailer;

	private AgentMailerAuthenticator authenticator;
	private String automatedSender;
	private List<InternetAddress> recipients;
	private Properties mailerProperties;

	private AgentMailer(Properties props) {
		String sender = props.getProperty("mail.sender.username");
		this.authenticator = new AgentMailerAuthenticator(sender, props.getProperty("mail.sender.password"));
		this.automatedSender = sender;

		this.recipients = new ArrayList<InternetAddress>();
		for (String email : props.getProperty("mail.recipients", "").split("\\|")) {
			try {
				this.recipients.add(new InternetAddress(email));
			} catch (AddressException e) {
				log.warn("Invalid email address found.  Ignoring.", e);
			}
		}

		props.put("mail.transport.protocol", "smtp");
		props.put("mail.smtp.host", props.getProperty("mail.host", ""));
		props.put("mail.smtp.port", props.getProperty("mail.port", ""));
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.starttls.enable", "true");
		mailerProperties = props;
	}

	public void notifyZookeeperOffline(String zookeeperName) {
		notifyNodeOffline("Zookeeper", zookeeperName);
	}

	public void notifyControllerOffline(String controllerName) {
		notifyNodeOffline("Controller", controllerName);
	}

	public void notifyControllerOffline(List<String> controllerNames) {
		if (controllerNames.size() == 1) {
			notifyNodeOffline("Controllers", controllerNames.get(0));
		} else {
			notifyMultipleNodeOffline("Controllers", controllerNames);
		}
	}

	public void notifyShardOffline(String shardName) {
		notifyNodeOffline("Shard", shardName);
	}

	public void notifyShardOffline(List<String> shardNames) {
		if (shardNames.size() == 1) {
			notifyNodeOffline("Shards", shardNames.get(0));
		} else {
			notifyMultipleNodeOffline("Shards", shardNames);
		}
	}

	private void notifyNodeOffline(String type, String name) {
		String subject = "Blur Console: " + type + " [" + name + "] may have gone offline!";
		String body = "Blur Console has received notice that " + type + " [" + name
				+ "] has recently gone offline, if this was expected please ignore this email.";
		sendMessage(subject, body);
	}

	private void notifyMultipleNodeOffline(String type, List<String> names) {
		String subject = "Blur Console: Multiple " + type + " may have gone offline!";
		String body = "Blur Console has received notice that " + type + " [" + StringUtils.join(names, "','")
				+ "] have recently gone offline, if this was expected please ignore this email.";
		sendMessage(subject, body);
	}

	private void sendMessage(String subject, String messageBody) {
		if (recipients.isEmpty()) {
			log.warn("There were no recipients found to send mail to.  Skipping send mail.");
			return;
		}

		Session session = Session.getInstance(this.mailerProperties, this.authenticator);
		try {
			MimeMessage message = new MimeMessage(session);
			message.setFrom(new InternetAddress(this.automatedSender));
			message.addRecipients(Message.RecipientType.TO, recipients.toArray(new InternetAddress[recipients.size()]));

			message.setSubject(subject);
			message.setContent(messageBody, "text/plain");

			Transport.send(message);
		} catch (MessagingException e) {
			log.error("An error occured while sending a warning email.", e);
		}
	}

	public static AgentMailer getMailer(Properties props, boolean forceNew) {
		if (mailer == null || forceNew) {
			mailer = new AgentMailer(props);
		}
		return mailer;
	}

	public static AgentMailer getMailer() {
		if (mailer != null) {
			return mailer;
		}

		log.warn("Mailer has not been configured yet.  No mail will be sent.");
		return new AgentMailer(new Properties());
	}
}
