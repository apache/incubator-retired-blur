#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

: <<DOCUMENTATION
 --------------------------------------------------------------------------
|                 control.sh 
 --------------------------------------------------------------------------
Script used by CM to control services of Blur. This should mostly be 
bridge
 --------------------------------------------------------------------------
DOCUMENTATION

this="${BASH_SOURCE-$0}"
bin=$(cd -P -- "$(dirname -- "$this")" && pwd -P)
script="$(basename -- "$this")"
this="$bin/$script"

set -e
set -u
set -o pipefail

IFS=$'\n\t'

scratch=$(mktemp -d -t scratch.tmp.XXXXXXXXXX)

function finish {
  rm -rf "${scratch}"
  # Your cleanup code here
}
trap finish EXIT

#__args
declare -A OPT_ARGS
usage() { echo "Usage: $0 <action>" 1>&2; exit 1; }

#__functions

write_to_blur_site_props() {
  prop="$1=$2"
  echo -n "Adding property[$prop] to blur-site.properties..."
  #For now, rely on last one in 
  echo $prop >> $BLUR_SITE
  echo " done."
}

setup_environment() {
	blur_tmp_path="${CONF_DIR}/tmp"
	blur_conf_dir="${CONF_DIR}"
	
	mkdir -p $blur_tmp_path
	mkdir -p $blur_conf_dir
	
	#Wacky, yo!
	chown blur:blur $blur_tmp_path
	
	BLUR_SITE=${blur_conf_dir}/blur-site.properties
	
	cp ${BLUR_HOME}/conf/log* ${blur_conf_dir}/
	
	if [ -z "$BLUR_ZK_ROOT" ]; then
	  echo "Rooting zookeeper at [${BLUR_ZK_ROOT}]"
	fi
	
	write_to_blur_site_props blur.zookeeper.connection "$ZK_QUORUM/$BLUR_ZK_ROOT"
		
	DFS_PATH=$(hdfs getconf -confKey fs.defaultFS)
	
	#TODO: Should allow BLUR_DEFAULT_TABLE_PATH to be absolute to another cluster too.
	write_to_blur_site_props "blur.cluster.${BLUR_CLUSTER_NAME}.table.uri" "$DFS_PATH/$BLUR_DEFAULT_TABLE_PATH"
	write_to_blur_site_props blur.command.lib.path "$DFS_PATH/$BLUR_COMMAND_PATH"
	
	export BLUR_CONF_DIR=$blur_conf_dir
	export BLUR_LOGS="/var/log/blur"
	export HADOOP_CLASSPATH=$(hadoop classpath)
}

start_controller() {
	setup_environment
	exec $BLUR_HOME/bin/start-supervised-controller-server.sh
}

start_shard() {
	setup_environment
	exec $BLUR_HOME/bin/start-supervised-shard-server.sh
}

#__main
action="$1"

if [ "${action}" == "" ] ;then
  usage
fi

echo "Executing [$action] with BLUR_HOME [$BLUR_HOME]"

export BLUR_LOGS=${CONF_DIR}/logs

case ${action} in
  (start-controller)
  	start_controller
    ;;
  (start-shard)
  	start_shard
    ;;    
  (*)
    echo "Unknown command[${action}]"
    ;;
esac






