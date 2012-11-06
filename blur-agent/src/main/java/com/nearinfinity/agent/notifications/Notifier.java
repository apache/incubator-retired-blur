package com.nearinfinity.agent.notifications;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Notifier {
	private static final Log log = LogFactory.getLog(Notifier.class);
	private static final String SUBJECT = "Blur Console: {0} nodes may have gone offline!";
	private static final String MESSAGE = "Blur Console has received notice that the following {0} nodes have recently gone offline.\n\n{1}";
	private static Notifier notifier;
	
	private Mailer mailer;
	private Messenger messenger;
	
	private Notifier(Properties props) {
		mailer = new Mailer(props);
		messenger = new Messenger(props);
	}
	
	public void notifyZookeeperOffline(String zookeeperName) {
		sendNotification("Zookeeper", new ArrayList<String>(Arrays.asList(new String[]{zookeeperName})));
	}

	public void notifyControllerOffline(List<String> controllerNames) {
		sendNotification("Controller", controllerNames);
	}

	public void notifyShardOffline(List<String> shardNames) {
		sendNotification("Shard", shardNames);
	}
	
	private void sendNotification(String nodeType, List<String> nodeNames) {
		String subject = MessageFormat.format(SUBJECT, nodeType);
		String message = MessageFormat.format(MESSAGE, nodeType, StringUtils.join(nodeNames, "\n"));
		
		mailer.sendMessage(subject, message);
		messenger.sendMessage(message);
	}
	
	public static Notifier getNotifier(Properties props, boolean forceNew) {
		if (notifier == null || forceNew) {
			notifier = new Notifier(props);
		}
		return notifier;
	}

	public static Notifier getNotifier() {
		if (notifier != null) {
			return notifier;
		}

		log.warn("Notifier has not been configured yet.  No notifications will be sent.");
		return new Notifier(new Properties());
	}
}
