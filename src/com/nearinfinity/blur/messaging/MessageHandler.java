package com.nearinfinity.blur.messaging;

public interface MessageHandler {
	byte[] handleMessage(byte[] message);
}
