package com.nearinfinity.blur.thrift;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import com.nearinfinity.blur.thrift.generated.BlurException;

//may not use....
public class ThriftExceptionHandler {
    
    @SuppressWarnings("unchecked")
    public static <T> T create(T instance, Class<T> type) {
        InvocationHandler handler = getHandler(instance);
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class[] {type}, handler);
    }
    
    private static <T> InvocationHandler getHandler(final T instance) {
        return new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                try {
                    return method.invoke(instance, args);
                } catch (Exception e) {
                    String message = getMessage(method,args,e);
                    if (e instanceof BlurException) {
                        throw e;
                    } else {
                        throw new BlurException(message);
                    }
                }
            }

            private String getMessage(Method method, Object[] args, Exception e) {
                return "";
            }
        };
    }
    

}
