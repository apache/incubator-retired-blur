package com.nearinfinity.blur.utils;

import java.util.Iterator;

public class IterableConverter<F,T> implements Iterable<T> {
	
	public static interface Converter<F,T> {
		T convert(F from) throws Exception;
	}

	private Converter<F, T> converter;
	private Iterable<F> iterable;
	
	public IterableConverter(Iterable<F> iterable, Converter<F,T> converter) {
		this.converter = converter;
		this.iterable = iterable;
	}

	@Override
	public Iterator<T> iterator() {
		final Iterator<F> iterator = iterable.iterator();
		return new Iterator<T>() {

			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public T next() {
				try {
					return converter.convert(iterator.next());
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			public void remove() {
				iterator.remove();
			}
		};
	}

}
