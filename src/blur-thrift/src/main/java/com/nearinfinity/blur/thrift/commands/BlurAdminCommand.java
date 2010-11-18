package com.nearinfinity.blur.thrift.commands;

import com.nearinfinity.blur.thrift.AbstractCommand;
import com.nearinfinity.blur.thrift.generated.BlurAdmin;

public abstract class BlurAdminCommand<T> extends AbstractCommand<BlurAdmin.Client, T> {

}
