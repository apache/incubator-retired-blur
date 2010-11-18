package com.nearinfinity.blur.thrift.commands;

import com.nearinfinity.blur.thrift.AbstractCommand;
import com.nearinfinity.blur.thrift.generated.Blur;

public abstract class BlurCommand<T> extends AbstractCommand<Blur.Client, T> {

}
