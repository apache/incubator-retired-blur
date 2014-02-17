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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Mailer {
	private static final Log log = LogFactory.getLog(Mailer.class);

	private AgentMailerAuthenticator authenticator;
	private String automatedSender;
	private List<InternetAddress> recipients;
	private Properties mailerProperties;
	private boolean sendMail = false;

	public Mailer(Properties props) {
		if (props.containsKey("mail.enabled") && props.getProperty("mail.enabled").equals("true")) {
			sendMail = true;
			authenticator = new AgentMailerAuthenticator(props.getProperty("mail.sender.username"), props.getProperty("mail.sender.password"));
			automatedSender = props.getProperty("mail.from.address", "DoNotReply");
			setupRecipients(props.getProperty("mail.recipients", ""));			
			
			mailerProperties = new Properties();
			mailerProperties.put("mail.transport.protocol", "smtp");
			mailerProperties.put("mail.smtp.host", props.getProperty("mail.host", ""));
			mailerProperties.put("mail.smtp.port", props.getProperty("mail.port", ""));
			mailerProperties.put("mail.smtp.auth", "true");
			mailerProperties.put("mail.smtp.starttls.enable", "true");
		}
	}

	public void sendMessage(String subject, String messageBody) {
		if (!sendMail) {
			log.info("Mailer has been disabled.  No emails will be sent.");
			return;
		}
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
	
	private void setupRecipients(String recipientProp) {
		recipients = new ArrayList<InternetAddress>();
		for (String email : recipientProp.split("\\|")) {
			try {
				recipients.add(new InternetAddress(email));
			} catch (AddressException e) {
				log.warn("Invalid email address found.  Ignoring.", e);
			}
		}
	}
}
