#!/usr/bin/env bash

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

function stopOnError {
  echo "#####################"
  echo "Running '$@'" >&2
  echo "#####################"
  "$@"
  local status=$?
  if [ $status -ne 0 ]; then
    echo "Error running '$@'" >&2
    exit $status
  fi
}

if [ -z "$JAVA_HOME" ]; then
  cat 1>&2 <<EOF
+======================================================================+
|      Error: JAVA_HOME is not set and Java could not be found         |
+----------------------------------------------------------------------+
| Please download the latest Sun JDK from the Sun Java web site        |
|       > http://java.sun.com/javase/downloads/ <                      |
|                                                                      |
| Hadoop and Blur requires Java 1.6 or later.                          |
| NOTE: This script will find Sun Java whether you install using the   |
|       binary or the RPM based installer.                             |
+======================================================================+
EOF
  exit 1
fi


version=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}')
if [[ "$version" == "1.6."* ]]; then
  echo Java Version is 1.6 ["$version"]
else
  echo Java Version not 1.6 actual ["$version"]
  exit 1
fi

version=$($JAVA_HOME/bin/java -version 2>&1 | awk -F '"' '/version/ {print $2}')
if [[ "$version" == "1.6."* ]]; then
  echo JAVA_HOME Java Version is 1.6 ["$version"]
else
  echo JAVA_HOME Java Version not 1.6 actual ["$version"]
  exit 1
fi

RELEASE_SCRIPTS_DIR=`dirname "$0"`
RELEASE_SCRIPTS_DIR=`cd "$RELEASE_SCRIPTS_DIR"; pwd`

PROJECT_BASE=$RELEASE_SCRIPTS_DIR/../

if [ -z "$1" ]; then
  echo "No output directory specified."
  exit 1
fi

OUTPUT_DIR=$1

if [ ! -d $OUTPUT_DIR ]; then
  echo "Output directory [${OUTPUT_DIR}] does not exist."
  exit 1
else
  stopOnError touch $OUTPUT_DIR/test
  stopOnError rm $OUTPUT_DIR/test
fi

PROFILE_FILE='release_profiles.txt'
PROFILE_LINES=`cat $PROFILE_FILE | grep -v \#`

for PROFILE in $PROFILE_LINES ; do
  cd $PROJECT_BASE
  HADOOP_VERSIONS_FILE="$RELEASE_SCRIPTS_DIR/${PROFILE}.versions"
  HADOOP_VERSIONS_LINES=`cat $HADOOP_VERSIONS_FILE | grep -v \#`
  for HADOOP_VERSION in $HADOOP_VERSIONS_LINES ; do
    stopOnError mvn clean -D${PROFILE} -Dhadoop.version=$HADOOP_VERSION -Djava.awt.headless=true
    stopOnError mvn install -D${PROFILE} -Dhadoop.version=$HADOOP_VERSION -Djava.awt.headless=true
    stopOnError mvn site -Ddependency.locations.enabled=false -DskipTests -D${PROFILE} -Dhadoop.version=$HADOOP_VERSION -Djava.awt.headless=true
    stopOnError mvn site:stage -DskipTests -D${PROFILE} -Dhadoop.version=$HADOOP_VERSION -Djava.awt.headless=true
    stopOnError mvn package -DskipTests -D${PROFILE} -Dhadoop.version=$HADOOP_VERSION -Djava.awt.headless=true
    stopOnError cp distribution/target/*-bin.tar.gz $OUTPUT_DIR
    stopOnError cp distribution/target/*-src.tar.gz $OUTPUT_DIR
  done
done





