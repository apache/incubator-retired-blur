package org.apache.blur.agent.monitor;

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
import java.util.Iterator;
import java.util.Set;

public class ThreadController {
	private static Set<Thread> threads = new HashSet<Thread>();
	public static boolean exitOnStop = true;

	public static void registerThread(Thread thread) {
		if (thread != null) {
			threads.add(thread);
		}
	}

	public static void stopAllThreads() {
		Iterator<Thread> iterator = threads.iterator();
		while (iterator.hasNext()) {
			Thread next = iterator.next();
			next.interrupt();
			iterator.remove();
		}

		if (exitOnStop) {
			System.exit(1);
		}
	}
}
