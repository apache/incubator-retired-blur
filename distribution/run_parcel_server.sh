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

PROJECT_DIR=`dirname "$0"`
PROJECT_DIR=`cd "$PROJECT_DIR"; pwd`

BLUR_VERSION=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version -Dhadoop2 | grep -Ev '(^\[|Download\w+:)'`
TARGET="${PROJECT_DIR}/target"

LAST_UPDATED_SEC=`date +%s`
LAST_UPDATED="${LAST_UPDATED_SEC}0000"

PARCEL="${TARGET}/blur-${BLUR_VERSION}.parcel.tar.gz"
PARCEL_SHA="${PARCEL}.sha"
BLUR_CSD="${TARGET}/BLUR-${BLUR_VERSION}.jar"

HTTP_DIR="${TARGET}/http"
MANIFEST="${HTTP_DIR}/manifest.json"

rm -r $HTTP_DIR
mkdir $HTTP_DIR

shasum $PARCEL | awk '{print $1}' > $PARCEL_SHA
HASH=`cat $PARCEL_SHA`
echo "{\"lastUpdated\":${LAST_UPDATED},\"parcels\": [" > $MANIFEST
for DISTRO in el5 el6 sles11 lucid precise trusty squeeze wheezy
do
	if [ $DISTRO != "el5" ] ; then
		echo "," >> $MANIFEST
	fi
	DISTRO_PARCEL="blur-${BLUR_VERSION}-${DISTRO}.parcel"
	DISTRO_PARCEL_SHA="blur-${BLUR_VERSION}-${DISTRO}.parcel.sha"
	ln $PARCEL "${HTTP_DIR}/${DISTRO_PARCEL}"
	ln $PARCEL_SHA "${HTTP_DIR}/${DISTRO_PARCEL_SHA}"
	echo "{\"parcelName\":\"${DISTRO_PARCEL}\",\"components\": [{\"name\" : \"blur\",\"version\" : \"${BLUR_VERSION}\",\"pkg_version\": \"${BLUR_VERSION}\"}],\"hash\":\"${HASH}\"}" >> $MANIFEST
done
echo "]}" >> $MANIFEST
CSD_BLUR_VERSION=`echo ${BLUR_VERSION} | tr - .`
ln $BLUR_CSD "${HTTP_DIR}/BLUR-${CSD_BLUR_VERSION}.jar"
cd ${HTTP_DIR}
python -m SimpleHTTPServer


