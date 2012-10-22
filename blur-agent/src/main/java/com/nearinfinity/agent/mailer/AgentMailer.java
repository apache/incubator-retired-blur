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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AgentMailer implements AgentMailerInterface {
  private static final Log log = LogFactory.getLog(AgentMailer.class);
  
  private final AgentMailerAuthenticator authenticator;
  private final String automatedSender;
  private final List<InternetAddress> recipients;

  public AgentMailer(String host, String port, String sender, String password, String recipients) throws AddressException {
    this.authenticator = new AgentMailerAuthenticator(sender, password);
    this.automatedSender = sender;
    
    this.recipients = new ArrayList<InternetAddress>();
    for (String email : recipients.split("\\|")) {
      this.recipients.add(new InternetAddress(email));
    }

    Properties props = System.getProperties();
    props.put("mail.transport.protocol", "smtp");
    props.put("mail.smtp.host", host);
    props.put("mail.smtp.port", port);
    props.put("mail.smtp.auth", "true");
    props.put("mail.smtp.starttls.enable", "true");
  }
  
  @Override
  public void notifyZookeeperOffline(String zookeeperName) {
    notifyNodeOffline("Zookeeper", zookeeperName);
  }

  @Override
  public void notifyControllerOffline(String controllerName) {
    notifyNodeOffline("Controller", controllerName);
  }

  @Override
  public void notifyShardOffline(String shardName) {
    notifyNodeOffline("Shard", shardName);
  }
  
  private void notifyNodeOffline(String type, String name){
    String subject = type + " [" + name + "] has gone offline!";
    String body = type + " [" + name + "] has recently gone offline, if this was expected please ignore this email.";
    sendMessage(subject, body);
  }

  private void sendMessage(String subject, String messageBody) {
    Session session = Session.getDefaultInstance(System.getProperties(), authenticator);
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
}
