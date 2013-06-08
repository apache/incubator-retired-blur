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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Properties;

import javax.mail.MessagingException;

import org.apache.blur.agent.notifications.Messenger;
import org.junit.Test;
import org.springframework.util.ReflectionUtils;


public class MessengerTest {

	@Test
	public void testSingletons() {
		// getter no props
		Messenger mailer = new Messenger(new Properties());
		assertFieldEquals("Send message should be false", mailer, "sendMessage", false);
		assertFieldEquals("Recipients shouldn't have been set because sendMessage is false.", mailer, "recipients", null);

		// getter with props
		Properties props = new Properties();
		props.setProperty("messenger.enabled", "true");
		props.setProperty("messenger.host", "localhost");
		props.setProperty("messenger.port", "25");
		props.setProperty("messenger.user", "crohr");
		props.setProperty("messenger.password", "password");
		props.setProperty("messenger.recipients", "crohr@nearinfinity.com|bmarcur@nearinfinity.com");
		mailer = new Messenger(props);
		assertFieldEquals("Send messenger should be true", mailer, "sendMessage", true);
		assertCollectionFieldEquals("There should be 2 valid recipients", mailer, "recipients", 2);
	}

	
	@Test
	public void testSendMessageMessengerDisabled() {
		XMPPEmbeddedServer.startServer(2500);
		
		Properties props = new Properties();
		props.setProperty("messenger.enabled", "false");
		Messenger mailer = new Messenger(props);
		mailer.sendMessage("Test Message");
		
		XMPPEmbeddedServer.stopServer();
		assertTrue(XMPPEmbeddedServer.getMessages().size() == 0);
	}
	
	@Test
	public void testSendMessageNoRecipients() {
		XMPPEmbeddedServer.startServer(2500);
		
		Properties props = new Properties();
		props.setProperty("messenger.enabled", "true");
		props.setProperty("messenger.host", "localhost");
		props.setProperty("messenger.port", "2500");
		props.setProperty("messenger.user", "crohr");
		props.setProperty("messenger.password", "password");
		Messenger mailer = new Messenger(props);
		mailer.sendMessage("Test Message");
		
		XMPPEmbeddedServer.stopServer();
		assertTrue(XMPPEmbeddedServer.getMessages().size() == 0);
	}
	
	@Test
	public void testSendMessage() throws MessagingException {
		XMPPEmbeddedServer.startServer(2500);
		
		Properties props = new Properties();
		props.setProperty("messenger.enabled", "true");
		props.setProperty("messenger.host", "localhost");
		props.setProperty("messenger.port", "2500");
		props.setProperty("messenger.user", "crohr");
		props.setProperty("messenger.password", "password");
		props.setProperty("messenger.recipients", "crohr@nearinfinity.com|bmarcur@nearinfinity.com");
		Messenger mailer = new Messenger(props);
		mailer.sendMessage("Test Message");
		
		XMPPEmbeddedServer.stopServer();
		assertTrue(XMPPEmbeddedServer.getMessages().size() == 0);
		//assertEquals("Test Message", XMPPEmbeddedServer.getMessages().get(0));
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
