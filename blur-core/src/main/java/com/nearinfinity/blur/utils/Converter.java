package com.nearinfinity.blur.utils;

public interface Converter<F,T> {
    T convert(F from) throws Exception;
}
