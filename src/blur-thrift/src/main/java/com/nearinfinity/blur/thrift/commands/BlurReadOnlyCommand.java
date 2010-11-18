package com.nearinfinity.blur.thrift.commands;

import com.nearinfinity.blur.thrift.AbstractCommand;
import com.nearinfinity.blur.thrift.generated.BlurReadOnly;

public abstract class BlurReadOnlyCommand<T> extends AbstractCommand<BlurReadOnly.Client, T> {

}
