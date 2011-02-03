package com.nearinfinity.blur.utils;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ForkJoin {
    
    private static Log LOG = LogFactory.getLog(ForkJoin.class);

	public static interface ParallelCall<INPUT, OUTPUT> {
		OUTPUT call(INPUT input) throws Exception;
	}

	public static interface ParallelReturn<OUTPUT> {
		OUTPUT merge(Merger<OUTPUT> merger) throws Exception;
	}

	public static interface Merger<OUTPUT> {
		OUTPUT merge(BlurExecutorCompletionService<OUTPUT> service) throws Exception;
	}

	public static <INPUT, OUTPUT> ParallelReturn<OUTPUT> execute(ExecutorService executor, Iterable<INPUT> it, final ParallelCall<INPUT, OUTPUT> parallelCall) {
		final BlurExecutorCompletionService<OUTPUT> service = new BlurExecutorCompletionService<OUTPUT>(executor);
		for (final INPUT input : it) {
		    service.submit(new Callable<OUTPUT>() {
                @Override
                public OUTPUT call() throws Exception {
                    return parallelCall.call(input);
                }
            });
		}
		return new ParallelReturn<OUTPUT>() {
			@Override
			public OUTPUT merge(Merger<OUTPUT> merger) throws Exception {
				return merger.merge(service);
			}
		};
	}

    public static <T> T ignoreExecutionException(Future<T> future, T defaultValue) throws InterruptedException {
        try {
            return future.get();
        } catch (ExecutionException e) {
            LOG.error("Error while trying to execute task [" + e.getMessage() + "]",e);
            return defaultValue;
        }
    }
}
