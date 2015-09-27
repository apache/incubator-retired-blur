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

cdir=`dirname "$0"`
cdir=`cd "$cdir"; pwd`

BASEDIR="${cdir}/../../"

cd $cdir
rm ${BASEDIR}/blur-thrift/src/main/java/org/apache/blur/thrift/generated/*
rm -r gen-java/ gen-perl/ gen-rb/ gen-html/
thrift --gen html --gen perl --gen java --gen rb --gen js Blur.thrift
for f in gen-java/org/apache/blur/thrift/generated/*.java; do
  awk -v f="apache.header" 'BEGIN {while (getline < f) txt=txt $0 "\n"} /package org\.apache\.blur\.thrift\.generated;/ {sub("package org.apache.blur.thrift.generated;", txt)} 1' $f > $f.new1
  sed 's/org\.apache\.thrift\./org\.apache\.blur\.thirdparty\.thrift_0_9_0\./g' $f.new1 > $f.new2
  sed 's/import\ org\.slf4j\.Logger/\/\/import\ org\.slf4j\.Logger/g' $f.new2 > $f.new3
  sed 's/private\ static\ final\ Logger\ LOGGER/\/\/private\ static\ final\ Logger\ LOGGER/g' $f.new3 > $f.new4
  rm $f.new1 $f.new2 $f.new3 $f
  mv $f.new4 $f
done
GEN_HTML=$cdir/gen-html/Blur.html
OUTPUT_HTML=${BASEDIR}/docs/Blur.html
cd ${BASEDIR}/blur-util
mvn clean install -DskipTests
mvn exec:java -Dexec.mainClass="org.apache.blur.doc.CreateBlurApiHtmlPage" -Dexec.args="$GEN_HTML $OUTPUT_HTML"
mvn clean -DskipTests
cd $cdir
cp -r gen-java/* ${BASEDIR}/blur-thrift/src/main/java/
cd ${BASEDIR}/blur-thrift
cp src/main/resources/org/apache/blur/thrift/generated/SafeClientGen.java.base src/main/java/org/apache/blur/thrift/generated/SafeClientGen.java
mvn clean install -DskipTests
mvn exec:java -Dexec.mainClass="org.apache.blur.thrift.util.GenerateSafeClient" -Dhadoop1
mvn clean -DskipTests
cd $cdir
echo "-----------------------------------------------"
echo "-----------------------------------------------"
echo "-----------------------------------------------"
echo "-----------------------------------------------"
echo "You will need to run 'mvn install -DskipTests'."
echo "-----------------------------------------------"
echo "-----------------------------------------------"
echo "-----------------------------------------------"
echo "-----------------------------------------------"
