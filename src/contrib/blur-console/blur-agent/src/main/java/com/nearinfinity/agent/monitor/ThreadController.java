package com.nearinfinity.agent.monitor;

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
