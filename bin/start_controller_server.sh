#!/bin/sh
. $BLUR_HOME/bin/blur_config.sh
pid=$BLUR_PID_DIR/blur_controller.pid
if [ -f $pid ]; then
  if kill -0 `cat $pid` > /dev/null 2>&1; then
    echo controller running as process `cat $pid`.  Stop it first.
    exit 1
  fi
fi

nohup java -cp $BLUR_HOME/conf:$BLUR_CLASS_PATH com.nearinfinity.blur.thrift.ThriftServer controller > $BLUR_LOG/blur_controller_server.out &
echo $! > $pid


