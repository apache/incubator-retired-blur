#!/bin/sh

bin=`dirname $0`
bin=`cd "$bin"; pwd`

export BLUR_AGENT_HOME="$bin"/..
export BLUR_AGENT_HOME_LIB=$BLUR_AGENT_HOME/lib

for f in $BLUR_AGENT_HOME_LIB/*.jar; do
  BLUR_AGENT_CLASSPATH="$BLUR_AGENT_CLASSPATH:$f"
done

cd $BLUR_AGENT_HOME/bin

$JAVA_HOME/bin/java -cp $BLUR_AGENT_CLASSPATH com.nearinfinity.agent.Agent $1
