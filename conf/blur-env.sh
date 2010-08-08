# Set Blur-specific environment variables here.

# The only required environment variable is JAVA_HOME.  All others are
# optional.  When running a distributed configuration it is best to
# set JAVA_HOME in this file, so that it is correctly defined on
# remote nodes.

# The java implementation to use.  Required.
export JAVA_HOME=/Library/Java/Home

# Extra Java CLASSPATH elements.  Optional.
# export BLUR_CLASSPATH=

# The maximum amount of heap to use, in MB. Default is 1000.
# export BLUR_HEAPSIZE=2000

# Extra Java runtime options.  Empty by default.
# export BLUR_OPTS=-server

# Command specific options appended to BLUR_OPTS when specified
export BLUR_NODE_OPTS="-Dcom.sun.management.jmxremote $BLUR_NODE_OPTS"
export BLUR_MASTER_OPTS="-Dcom.sun.management.jmxremote $BLUR_MASTER_OPTS"

# Extra ssh options.  Empty by default.
# export BLUR_SSH_OPTS="-o ConnectTimeout=1 -o SendEnv=BLUR_CONF_DIR"

# Where log files are stored.  $BLUR_HOME/logs by default.
# export BLUR_LOG_DIR=${BLUR_HOME}/logs

# File naming remote slave hosts.  $HADOOP_HOME/conf/slaves by default.
# export BLUR_NODES=${BLUR_HOME}/conf/nodes

# Seconds to sleep between slave commands.  Unset by default.  This
# can be useful in large clusters, where, e.g., slave rsyncs can
# otherwise arrive faster than the master can service them.
# export BLUR_NODE_SLEEP=0.1

# The directory where pid files are stored. /tmp by default.
# export BLUR_PID_DIR=/var/blur/pids

# A string representing this instance of blur. $USER by default.
# export BLUR_IDENT_STRING=$USER

# The scheduling priority for daemon processes.  See 'man nice'.
# export BLUR_NICENESS=10
