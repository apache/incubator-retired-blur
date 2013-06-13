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

version=$1

# Setup build directory
if [ ! -e "build" ]; then
  echo "Creating build directory"
  mkdir "build"
fi

# Clean build directory
echo "Cleaning build directory"
rm -r build/*

##########################
# Prep Rails app         #
##########################
echo "Assembling Rails app"
cd blur-admin
bundle install

echo "Compiling assets"
bundle exec rake RAILS_ENV=production js:routes
bundle exec rake RAILS_ENV=production assets:clean
bundle exec rake RAILS_ENV=production assets:precompile

cd ../build
GUI_DIR=gui
mkdir $GUI_DIR

echo "Copying Rails files"
cp ../blur-admin/Rakefile ../blur-admin/config.ru $GUI_DIR

echo "Copying app"
cp -r ../blur-admin/app $GUI_DIR

echo "Copying config"
cp -r ../blur-admin/config $GUI_DIR
rm $GUI_DIR/config/environments/development.rb
rm $GUI_DIR/config/environments/test.rb

echo "Copying db"
cp -r ../blur-admin/db $GUI_DIR

echo "Copying lib"
cp -r ../blur-admin/lib $GUI_DIR
rm -r $GUI_DIR/lib/pounder
rm -r $GUI_DIR/lib/tasks/*.rake

echo "Copying public"
cp -r ../blur-admin/public $GUI_DIR

echo "Copying script"
cp -r ../blur-admin/script $GUI_DIR

mkdir $GUI_DIR/tmp
touch $GUI_DIR/tmp/placeholder.txt

echo "Copying vendor"
cp -r ../blur-admin/vendor $GUI_DIR

echo "Copy production files"
cp ../etc/default/Gemfile $GUI_DIR/
cp ../etc/default/database.yml $GUI_DIR/config/

echo "Vendor gems"
cd $GUI_DIR
bundle install
bundle package

find . -name .DS_Store | xargs rm

echo "Compressing and zipping rails dir"
cd ..
mv $GUI_DIR "$GUI_DIR-$version"
tar -cvzf "$GUI_DIR-$version.tar.gz" "$GUI_DIR-$version"

#################################
# Prep Agent                    #
#################################

echo "Build agent jar"
cd ../blur-agent
mvn clean package -DskipTests
cd ../build

mkdir agent

echo "Create bin dir"
mkdir agent/bin
cp ../etc/*.sh agent/bin
chmod 775 agent/bin/*.sh

echo "Create conf dir"
mkdir agent/conf
cp ../etc/agent.config.sample agent/conf
cp ../etc/log4j.properties agent/conf 

echo "Create lib dir"
mkdir agent/lib
cp ../blur-agent/target/blur-agent-*.jar agent/lib/agent.jar
cp ../blur-agent/target/lib/*.jar agent/lib
rm agent/lib/ant*.jar
rm agent/lib/commons-cli*.jar
rm agent/lib/commons-el*.jar
rm agent/lib/commons-httpclient*.jar
rm agent/lib/commons-net*.jar
rm agent/lib/core*.jar
rm agent/lib/hamcrest*.jar
rm agent/lib/hsqldb*.jar
rm agent/lib/jasper*.jar
rm agent/lib/jets3t*.jar
rm agent/lib/jetty*.jar
rm agent/lib/jsp-api*.jar
rm agent/lib/junit*.jar
rm agent/lib/mysql-connector-java*.jar
rm agent/lib/oro*.jar
rm agent/lib/servlet-api*.jar
rm agent/lib/xmlenc*.jar

echo "Compressing and zipping agent dir"
mv agent "agent-$version"
tar -cvzf "agent-$version.tar.gz" "agent-$version"

#################################
# Final packaging               #
#################################

cp ../etc/INSTALL .
cp ../etc/install.sh .
cp ../etc/VERSION .
tar -cvzf "blur-tools-$version.tar.gz" "$GUI_DIR-$version.tar.gz" "agent-$version.tar.gz" INSTALL VERSION

echo "Build complete"
