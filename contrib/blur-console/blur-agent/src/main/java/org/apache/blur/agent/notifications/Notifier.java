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
	
	private Notifier(Properties props) {
		mailer = new Mailer(props);
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
