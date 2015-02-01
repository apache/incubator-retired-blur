package org.apache.blur.thrift.sasl;

import java.net.InetSocketAddress;

import javax.security.sasl.AuthenticationException;

import org.apache.blur.BlurConfiguration;

public abstract class PasswordAuthenticationProvider {

  public PasswordAuthenticationProvider(BlurConfiguration configuration) {

  }

  public abstract void authenticate(String username, String password, InetSocketAddress address) throws AuthenticationException;

}