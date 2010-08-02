package com.nearinfinity.blur.utils;

import java.lang.instrument.Instrumentation;

public class ObjectSize {

	private static Instrumentation instrumentation;
	
	public static void premain(String agentArgs, Instrumentation inst) {
		instrumentation = inst;
	}
	
	public static long getSizeInBytes(Object o) {
		if (instrumentation == null) return -1;
		if (o == null) return 0;
		return instrumentation.getObjectSize(o);
	}
	
}
