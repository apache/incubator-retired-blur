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

if [ -z "$1" ]; then
  echo "No output directory specified."
fi
OUTPUT_DIR=$1
mvn clean -Dhadoop1
mvn install -Dhadoop1 -Djava.awt.headless=true
mvn site -Ddependency.locations.enabled=false -DskipTests -Dhadoop1 -Djava.awt.headless=true
mvn site:stage -DskipTests -Dhadoop1 -Djava.awt.headless=true
mvn package -DskipTests -Dhadoop1 -Djava.awt.headless=true
cp distribution/target/*-bin.tar.gz $OUTPUT_DIR
cp distribution/target/*-src.tar.gz $OUTPUT_DIR

mvn clean -Dhadoop2
mvn install -Dhadoop2 -Djava.awt.headless=true
mvn site -Ddependency.locations.enabled=false -DskipTests -Dhadoop2 -Djava.awt.headless=true
mvn site:stage -DskipTests -Dhadoop2 -Djava.awt.headless=true
mvn package -DskipTests -Dhadoop2 -Djava.awt.headless=true
cp distribution/target/*-bin.tar.gz $OUTPUT_DIR