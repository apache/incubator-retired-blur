package org.apache.blur.agent.notifications;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Properties;

import javax.mail.MessagingException;

import org.apache.blur.agent.notifications.Mailer;
import org.junit.Test;
import org.springframework.util.ReflectionUtils;
import org.subethamail.wiser.Wiser;


public class MailerTest {

	@Test
	public void testSingletons() {
		// getter no props
		Mailer mailer = new Mailer(new Properties());
		assertFieldEquals("Send mail should be false", mailer, "sendMail", false);
		assertFieldEquals("Recipients shouldn't have been set because sendMail is false.", mailer, "recipients", null);
		assertFieldEquals("Sender should be null", mailer, "automatedSender", null);

		// getter with props
		Properties props = new Properties();
		props.setProperty("mail.enabled", "true");
		props.setProperty("mail.host", "localhost");
		props.setProperty("mail.port", "25");
		props.setProperty("mail.sender.username", "crohr");
		props.setProperty("mail.from.address", "crohr");
		props.setProperty("mail.sender.password", "password");
		props.setProperty("mail.recipients", "crohr@nearinfinity.com|bmarcur@nearinfinity.com");
		mailer = new Mailer(props);
		assertFieldEquals("Send mail should be true", mailer, "sendMail", true);
		assertCollectionFieldEquals("There should be 2 valid recipients", mailer, "recipients", 2);
		assertFieldEquals("Sender should be crohr", mailer, "automatedSender", "crohr");
	}

	@Test
	public void testMailerSetupInvalidAddress() {
		Properties props = new Properties();
		props.setProperty("mail.enabled", "true");
		props.setProperty("mail.host", "localhost");
		props.setProperty("mail.port", "25");
		props.setProperty("mail.sender.username", "crohr");
		props.setProperty("mail.from.address", "crohr");
		props.setProperty("mail.sender.password", "password");
		props.setProperty("mail.recipients", "test@nearinfinity com");
		Mailer mailer = new Mailer(props);

		assertCollectionFieldEquals("There should be no valid recipients", mailer, "recipients", 0);
		assertFieldEquals("Sender should be crohr", mailer, "automatedSender", "crohr");
	}

	@Test
	public void testMailerSetupWithUniqueDomain() {
		Properties props = new Properties();
		props.setProperty("mail.enabled", "true");
		props.setProperty("mail.host", "localhost");
		props.setProperty("mail.port", "25");
		props.setProperty("mail.sender.username", "crohr");
		props.setProperty("mail.from.address", "crohr");
		props.setProperty("mail.sender.password", "password");
		props.setProperty("mail.recipients", "crohr@abc");
		Mailer mailer = new Mailer(props);

		assertCollectionFieldEquals("There should be 1 valid recipient", mailer, "recipients", 1);
		assertFieldEquals("Sender should be crohr", mailer, "automatedSender", "crohr");
	}
	
	@Test
	public void testSendMessageMailerDisabled() {
		Wiser server = new Wiser(2500);
		server.start();
		
		Properties props = new Properties();
		props.setProperty("mail.enabled", "false");
		Mailer mailer = new Mailer(props);
		mailer.sendMessage("Test Message Subject", "Test Message Body");
		
		server.stop();
		assertTrue(server.getMessages().size() == 0);
	}
	
	@Test
	public void testSendMessageNoRecipients() {
		Wiser server = new Wiser(2500);
		server.start();
		
		Properties props = new Properties();
		props.setProperty("mail.enabled", "true");
		props.setProperty("mail.host", "localhost");
		props.setProperty("mail.port", "25");
		props.setProperty("mail.sender.username", "crohr");
		props.setProperty("mail.from.address", "crohr");
		props.setProperty("mail.sender.password", "password");
		Mailer mailer = new Mailer(props);
		mailer.sendMessage("Test Message Subject", "Test Message Body");
		
		server.stop();
		assertTrue(server.getMessages().size() == 0);
	}
	
	@Test
	public void testSendMessage() throws MessagingException {
		Wiser server = new Wiser(2500);
		server.start();
		
		Properties props = new Properties();
		props.setProperty("mail.enabled", "true");
		props.setProperty("mail.host", "localhost");
		props.setProperty("mail.port", "2500");
		props.setProperty("mail.sender.username", "crohr");
		props.setProperty("mail.from.address", "crohr");
		props.setProperty("mail.sender.password", "password");
		props.setProperty("mail.recipients", "crohr@nearinfinity.com|bmarcur@nearinfinity.com");
		Mailer mailer = new Mailer(props);
		mailer.sendMessage("Test Message Subject", "Test Message Body");
		
		server.stop();
		assertTrue(server.getMessages().size() == 2);
		assertEquals("Test Message Subject", server.getMessages().get(0).getMimeMessage().getSubject());
	}

//	@Test
//	public void testSendZookeeperNotice() throws MessagingException, IOException {
//		Wiser server = new Wiser(2500);
//		server.start();
//
//		setupActiveMailer("crohr@nearinfinity.com").notifyZookeeperOffline("ZK1");
//
//		server.stop();
//
//		assertProperMessageSent("Zookeeper", "ZK1", server);
//	}
//	
//	@Test
//	public void testSendControllersNoticeSingleNode() throws MessagingException, IOException {
//		Wiser server = new Wiser(2500);
//		server.start();
//
//		List<String> names = new ArrayList<String>();
//		names.add("C1");
//		
//		setupActiveMailer("crohr@nearinfinity.com").notifyControllerOffline(names);
//
//		server.stop();
//
//		assertProperMessageSent("Controllers", "C1", server);
//	}
//	
//	@Test
//	public void testSendControllersNoticeMultiNode() throws MessagingException, IOException {
//		Wiser server = new Wiser(2500);
//		server.start();
//
//		List<String> names = new ArrayList<String>();
//		names.add("C1");
//		names.add("C2");
//		
//		setupActiveMailer("crohr@nearinfinity.com").notifyControllerOffline(names);
//
//		server.stop();
//
//		assertProperMultiMessageSent("Controllers", names, server);
//	}
//	
//	@Test
//	public void testSendShardsNoticeSingleNode() throws MessagingException, IOException {
//		Wiser server = new Wiser(2500);
//		server.start();
//
//		List<String> names = new ArrayList<String>();
//		names.add("S1");
//		
//		setupActiveMailer("crohr@nearinfinity.com").notifyShardOffline(names);
//
//		server.stop();
//
//		assertProperMessageSent("Shards", "S1", server);
//	}
//	
//	@Test
//	public void testSendShardsNoticeMultiNode() throws MessagingException, IOException {
//		Wiser server = new Wiser(2500);
//		server.start();
//
//		List<String> names = new ArrayList<String>();
//		names.add("S1");
//		names.add("S2");
//		
//		setupActiveMailer("crohr@nearinfinity.com").notifyShardOffline(names);
//
//		server.stop();
//
//		assertProperMultiMessageSent("Shards", names, server);
//	}
//
//	@Test
//	public void testNoMessageSentWithNoRecipients() {
//		Wiser server = new Wiser(2500);
//		server.start();
//
//		List<String> names = new ArrayList<String>();
//		names.add("S1");
//		setupActiveMailer("").notifyShardOffline(names);
//
//		server.stop();
//
//		assertTrue(server.getMessages().size() == 0);
//	}

//	private Mailer setupActiveMailer(String recipients) {
//		Properties props = new Properties();
//		props.setProperty("mail.host", "localhost");
//		props.setProperty("mail.port", "2500");
//		props.setProperty("mail.sender.username", "crohr");
//		props.setProperty("mail.sender.password", "password");
//		props.setProperty("mail.recipients", recipients);
//		return new Mailer(props);
//	}
//
//	private void assertProperMessageSent(String type, String name, Wiser server) throws MessagingException, IOException {
//		assertTrue(server.getMessages().size() == 1);
//		Iterator<WiserMessage> emailIter = server.getMessages().iterator();
//		WiserMessage email = (WiserMessage) emailIter.next();
//		assertTrue(email.getMimeMessage().getSubject().equals("Blur Console: " + type + " [" + name + "] may have gone offline!"));
//		assertEquals("Blur Console has received notice that " + type + " [" + name
//				+ "] has recently gone offline, if this was expected please ignore this email.",
//				StringUtils.trim((String) email.getMimeMessage().getContent()));
//	}
//	
//	private void assertProperMultiMessageSent(String type, List<String> names, Wiser server) throws MessagingException, IOException {
//		assertTrue(server.getMessages().size() == 1);
//		Iterator<WiserMessage> emailIter = server.getMessages().iterator();
//		WiserMessage email = (WiserMessage) emailIter.next();
//		assertTrue(email.getMimeMessage().getSubject().equals("Blur Console: Multiple " + type + " may have gone offline!"));
//		assertEquals("Blur Console has received notice that " + type + " [" + StringUtils.join(names, "','")
//				+ "] have recently gone offline, if this was expected please ignore this email.",
//				StringUtils.trim((String) email.getMimeMessage().getContent()));
//	}

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
