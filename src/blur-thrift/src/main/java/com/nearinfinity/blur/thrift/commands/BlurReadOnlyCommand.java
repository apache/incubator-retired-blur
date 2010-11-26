package com.nearinfinity.blur.thrift.commands;

import com.nearinfinity.blur.thrift.AbstractCommand;
import com.nearinfinity.blur.thrift.generated.BlurUpdate;

public abstract class BlurReadOnlyCommand<T> extends AbstractCommand<BlurUpdate.Client, T> {

}
