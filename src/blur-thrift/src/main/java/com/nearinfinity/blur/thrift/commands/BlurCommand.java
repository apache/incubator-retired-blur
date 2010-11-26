package com.nearinfinity.blur.thrift.commands;

import com.nearinfinity.blur.thrift.AbstractCommand;
import com.nearinfinity.blur.thrift.generated.BlurSearch;

public abstract class BlurCommand<T> extends AbstractCommand<BlurSearch.Client, T> {

}
