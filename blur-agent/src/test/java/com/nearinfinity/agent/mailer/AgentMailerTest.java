package com.nearinfinity.agent.mailer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import javax.mail.MessagingException;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.springframework.util.ReflectionUtils;
import org.subethamail.wiser.Wiser;
import org.subethamail.wiser.WiserMessage;

public class AgentMailerTest {

	@Test
	public void testSingletons() {
		// getter no props
		AgentMailer mailer = AgentMailer.getMailer();
		assertCollectionFieldEquals("There should be no valid recipients", mailer, "recipients", 0);
		assertFieldEquals("Sender should be null", mailer, "automatedSender", null);

		// getter with props
		Properties props = new Properties();
		props.setProperty("mail.host", "localhost");
		props.setProperty("mail.port", "25");
		props.setProperty("mail.sender.username", "crohr");
		props.setProperty("mail.sender.password", "password");
		props.setProperty("mail.recipients", "crohr@nearinfinity.com|bmarcur@nearinfinity.com");
		mailer = AgentMailer.getMailer(props, false);
		assertCollectionFieldEquals("There should be 2 valid recipients", mailer, "recipients", 2);
		assertFieldEquals("Sender should be crohr", mailer, "automatedSender", "crohr");

		// getter with props after first initialization
		mailer = AgentMailer.getMailer(props, false);
		assertCollectionFieldEquals("There should be 2 valid recipients", mailer, "recipients", 2);
		assertFieldEquals("Sender should be crohr", mailer, "automatedSender", "crohr");

		// getter no props after first initialization
		mailer = AgentMailer.getMailer();
		assertCollectionFieldEquals("There should be 2 valid recipients", mailer, "recipients", 2);
		assertFieldEquals("Sender should be crohr", mailer, "automatedSender", "crohr");

		// getter with props force reload
		props.setProperty("mail.recipients", "test@nearinfinity.com");
		props.setProperty("mail.sender.username", "bmarcur");
		mailer = AgentMailer.getMailer(props, true);
		assertCollectionFieldEquals("There should be 1 valid recipient", mailer, "recipients", 1);
		assertFieldEquals("Sender should be bmarcur", mailer, "automatedSender", "bmarcur");

	}

	@Test
	public void testMailerSetup() {
		Properties props = new Properties();
		props.setProperty("mail.host", "localhost");
		props.setProperty("mail.port", "25");
		props.setProperty("mail.sender.username", "crohr");
		props.setProperty("mail.sender.password", "password");
		props.setProperty("mail.recipients", "crohr@nearinfinity.com|bmarcur@nearinfinity.com");
		AgentMailer mailer = AgentMailer.getMailer(props, true);

		assertCollectionFieldEquals("There should be 2 valid recipients", mailer, "recipients", 2);
		assertFieldEquals("Sender should be crohr", mailer, "automatedSender", "crohr");
	}

	@Test
	public void testMailerSetupInvalidAddress() {
		Properties props = new Properties();
		props.setProperty("mail.host", "localhost");
		props.setProperty("mail.port", "25");
		props.setProperty("mail.sender.username", "crohr");
		props.setProperty("mail.sender.password", "password");
		props.setProperty("mail.recipients", "test@nearinfinity com");
		AgentMailer mailer = AgentMailer.getMailer(props, true);

		assertCollectionFieldEquals("There should be no valid recipients", mailer, "recipients", 0);
		assertFieldEquals("Sender should be crohr", mailer, "automatedSender", "crohr");
	}

	@Test
	public void testMailerSetupWithUniqueDomain() {
		Properties props = new Properties();
		props.setProperty("mail.host", "localhost");
		props.setProperty("mail.port", "25");
		props.setProperty("mail.sender.username", "crohr");
		props.setProperty("mail.sender.password", "password");
		props.setProperty("mail.recipients", "crohr@abc");
		AgentMailer mailer = AgentMailer.getMailer(props, true);

		assertCollectionFieldEquals("There should be 1 valid recipient", mailer, "recipients", 1);
		assertFieldEquals("Sender should be crohr", mailer, "automatedSender", "crohr");
	}

	@Test
	public void testSendZookeeperNotice() throws MessagingException, IOException {
		Wiser server = new Wiser(2500);
		server.start();

		setupActiveMailer("crohr@nearinfinity.com").notifyZookeeperOffline("ZK1");

		server.stop();

		assertProperMessageSent("Zookeeper", "ZK1", server);
	}

	@Test
	public void testSendControllerNotice() throws MessagingException, IOException {
		Wiser server = new Wiser(2500);
		server.start();

		setupActiveMailer("crohr@nearinfinity.com").notifyControllerOffline("C1");

		server.stop();

		assertProperMessageSent("Controller", "C1", server);
	}
	
	@Test
	public void testSendControllersNoticeSingleNode() throws MessagingException, IOException {
		Wiser server = new Wiser(2500);
		server.start();

		List<String> names = new ArrayList<String>();
		names.add("C1");
		
		setupActiveMailer("crohr@nearinfinity.com").notifyControllerOffline(names);

		server.stop();

		assertProperMessageSent("Controllers", "C1", server);
	}
	
	@Test
	public void testSendControllersNoticeMultiNode() throws MessagingException, IOException {
		Wiser server = new Wiser(2500);
		server.start();

		List<String> names = new ArrayList<String>();
		names.add("C1");
		names.add("C2");
		
		setupActiveMailer("crohr@nearinfinity.com").notifyControllerOffline(names);

		server.stop();

		assertProperMultiMessageSent("Controllers", names, server);
	}

	@Test
	public void testSendShardNotice() throws MessagingException, IOException {
		Wiser server = new Wiser(2500);
		server.start();

		setupActiveMailer("crohr@nearinfinity.com").notifyShardOffline("S1");

		server.stop();

		assertProperMessageSent("Shard", "S1", server);
	}
	
	@Test
	public void testSendShardsNoticeSingleNode() throws MessagingException, IOException {
		Wiser server = new Wiser(2500);
		server.start();

		List<String> names = new ArrayList<String>();
		names.add("S1");
		
		setupActiveMailer("crohr@nearinfinity.com").notifyShardOffline(names);

		server.stop();

		assertProperMessageSent("Shards", "S1", server);
	}
	
	@Test
	public void testSendShardsNoticeMultiNode() throws MessagingException, IOException {
		Wiser server = new Wiser(2500);
		server.start();

		List<String> names = new ArrayList<String>();
		names.add("S1");
		names.add("S2");
		
		setupActiveMailer("crohr@nearinfinity.com").notifyShardOffline(names);

		server.stop();

		assertProperMultiMessageSent("Shards", names, server);
	}

	@Test
	public void testNoMessageSentWithNoRecipients() {
		Wiser server = new Wiser(2500);
		server.start();

		setupActiveMailer("").notifyShardOffline("S1");

		server.stop();

		assertTrue(server.getMessages().size() == 0);
	}

	private AgentMailer setupActiveMailer(String recipients) {
		Properties props = new Properties();
		props.setProperty("mail.host", "localhost");
		props.setProperty("mail.port", "2500");
		props.setProperty("mail.sender.username", "crohr");
		props.setProperty("mail.sender.password", "password");
		props.setProperty("mail.recipients", recipients);
		return AgentMailer.getMailer(props, true);
	}

	private void assertProperMessageSent(String type, String name, Wiser server) throws MessagingException, IOException {
		assertTrue(server.getMessages().size() == 1);
		Iterator<WiserMessage> emailIter = server.getMessages().iterator();
		WiserMessage email = (WiserMessage) emailIter.next();
		assertTrue(email.getMimeMessage().getSubject().equals("Blur Console: " + type + " [" + name + "] may have gone offline!"));
		assertEquals("Blur Console has received notice that " + type + " [" + name
				+ "] has recently gone offline, if this was expected please ignore this email.",
				StringUtils.trim((String) email.getMimeMessage().getContent()));
	}
	
	private void assertProperMultiMessageSent(String type, List<String> names, Wiser server) throws MessagingException, IOException {
		assertTrue(server.getMessages().size() == 1);
		Iterator<WiserMessage> emailIter = server.getMessages().iterator();
		WiserMessage email = (WiserMessage) emailIter.next();
		assertTrue(email.getMimeMessage().getSubject().equals("Blur Console: Multiple " + type + " may have gone offline!"));
		assertEquals("Blur Console has received notice that " + type + " [" + StringUtils.join(names, "','")
				+ "] have recently gone offline, if this was expected please ignore this email.",
				StringUtils.trim((String) email.getMimeMessage().getContent()));
	}

	private void assertFieldEquals(String message, Object object, String fieldName, Object value) {
		Field field = ReflectionUtils.findField(object.getClass(), fieldName);
		ReflectionUtils.makeAccessible(field);
		assertEquals(message, value, ReflectionUtils.getField(field, object));
	}

	@SuppressWarnings("unchecked")
	private void assertCollectionFieldEquals(String message, Object object, String fieldName, int expectedCollectionSize) {
		Field field = ReflectionUtils.findField(object.getClass(), fieldName);
		ReflectionUtils.makeAccessible(field);
		assertEquals(message, expectedCollectionSize, ((Collection<Object>) ReflectionUtils.getField(field, object)).size());
	}
}
