package com.nearinfinity.blur.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class ForkJoin {

	public static interface ParallelCall<INPUT, OUTPUT> {
		OUTPUT call(INPUT input) throws Exception;
	}

	public static interface ParallelReturn<OUTPUT> {
		OUTPUT merge(Merger<OUTPUT> merger) throws Exception;
	}

	public static interface Merger<OUTPUT> {
		OUTPUT merge(List<Future<OUTPUT>> futures) throws Exception;
	}

	public static <INPUT, OUTPUT> ParallelReturn<OUTPUT> execute(ExecutorService executor, Iterable<INPUT> it, final ParallelCall<INPUT, OUTPUT> parallelCall) {
		final List<Future<OUTPUT>> outputs = new ArrayList<Future<OUTPUT>>();
		for (final INPUT input : it) {
			outputs.add(executor.submit(new Callable<OUTPUT>() {
				@Override
				public OUTPUT call() throws Exception {
					return parallelCall.call(input);
				}
			}));
		}
		return new ParallelReturn<OUTPUT>() {
			@Override
			public OUTPUT merge(Merger<OUTPUT> merger) throws Exception {
				return merger.merge(outputs);
			}
		};
	}
}
