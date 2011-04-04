package com.nearinfinity.blur.thrift;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.text.MessageFormat;

import com.nearinfinity.blur.thrift.generated.BlurException;

public class BException extends BlurException {

    private static final long serialVersionUID = 5846541677293727358L;

    public BException(String message, Throwable t) {
        super(message, toString(t));
    }
    
    public BException(String message, Object... parameters) {
        this(MessageFormat.format(message.toString(), parameters), (Throwable) null);
    }
    
    public BException(String message, Throwable t, Object... parameters) {
        this(MessageFormat.format(message.toString(), parameters),t);
    }
    
    public static String toString(Throwable t) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter(outputStream);
        t.printStackTrace(writer);
        writer.close();
        return new String(outputStream.toByteArray());
    }
}
