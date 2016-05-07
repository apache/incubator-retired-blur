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

URL="<URL HERE>"
REPO_ID="snapshots"

#mvn install -D${PROFILE} -DskipTests
#[ $? -eq 0 ] || exit $?;

CUR_DIR=`pwd`
for FILE in *; do
  if [ -d $FILE ]
  then
    if [ -f $FILE/pom.xml ]
    then
      echo "#######################################"
      echo "# Deploying $FILE"
      echo "#######################################"
      
      cd $FILE

      VERSION=`mvn help:evaluate -Dexpression=project.version | grep -v "\[INFO\]" | grep -v "\[WARNING\]"`
      ARTIFACT=`mvn help:evaluate -Dexpression=project.artifactId | grep -v "\[INFO\]" | grep -v "\[WARNING\]"`

      JAR="target/${ARTIFACT}-${VERSION}.jar"
      JAR_SOURCES="target/${ARTIFACT}-${VERSION}-sources.jar"
      TESTS_JAR="target/${ARTIFACT}-${VERSION}-tests.jar"
      if [ -f $JAR ]
      then
        if [ -f target/effective-pom.xml ]
        then
          echo "Args PWD=$PWD REPO_ID=${REPO_ID} URL=${URL} ARTIFACT=${ARTIFACT} VERSION=${VERSION}"
          if [ -f $TESTS_JAR ]
          then
            mvn deploy:deploy-file -DrepositoryId=${REPO_ID} -Durl=${URL} -Dfile=$JAR -DpomFile=target/effective-pom.xml -Dtypes=jar -Dclassifiers=tests -Dfiles=$TESTS_JAR -Dsources=$JAR_SOURCES 
          else
            mvn deploy:deploy-file -DrepositoryId=${REPO_ID} -Durl=${URL} -Dfile=$JAR -DpomFile=target/effective-pom.xml 
          fi
          [ $? -eq 0 ] || exit $?;
        else 
          echo "No effective-pom.xml to deploy, SKIPPING."
        fi
      fi
      cd $CUR_DIR
    fi
  fi
done

