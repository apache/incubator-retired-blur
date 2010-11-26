package com.nearinfinity.blur.utils;

import java.util.Iterator;

public class IterableConverter<F,T> implements Iterable<T> {
	
	private Converter<F, T> converter;
	private Iterable<F> iterable;
	
	public IterableConverter(Iterable<F> iterable, Converter<F,T> converter) {
		this.converter = converter;
		this.iterable = iterable;
	}

	@Override
	public Iterator<T> iterator() {
		return new IteratorConverter<F,T>(iterable.iterator(),converter);
	}

}
