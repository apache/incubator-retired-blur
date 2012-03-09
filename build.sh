#!/bin/bash

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
mkdir rails

echo "Copying Rails files"
cp ../blur-admin/Rakefile ../blur-admin/config.ru rails

echo "Copying app"
cp -r ../blur-admin/app rails

echo "Copying config"
cp -r ../blur-admin/config rails
rm rails/config/environments/development.rb
rm rails/config/environments/test.rb

echo "Copying db"
cp -r ../blur-admin/db rails

echo "Copying lib"
cp -r ../blur-admin/lib rails
rm -r rails/lib/pounder
rm -r rails/lib/tasks/*.rake

echo "Copying public"
cp -r ../blur-admin/public rails

echo "Copying script"
cp -r ../blur-admin/script rails

mkdir rails/tmp
touch rails/tmp/placeholder.txt

echo "Copying vendor"
cp -r ../blur-admin/vendor rails

echo "Copy production files"
cp ../etc/default/Gemfile rails/
cp ../etc/default/database.yml rails/config/

if [ -n "$2" ] && [ $2 = "--certs" ]; then
  echo "Overlaying Cert Auth"
  cp -r ../etc/cert-auth/proof-0.1.0 rails/vendor/gems/
  cp ../etc/cert-auth/certificate-authentication.rb rails/config/initializers/
  cp ../etc/cert-auth/Gemfile rails/
  cp ../etc/cert-auth/production.rb rails/config/environments/
fi

echo "Vendor gems"
cd rails
bundle install
bundle package

find . -name .DS_Store | xargs rm

echo "Compressing and zipping rails dir"
cd ..
mv rails "rails-$version"
tar -cvzf "blur-console-$version.tar.gz" "rails-$version"

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
tar -cvzf "blur-tools-$version.tar.gz" "blur-console-$version.tar.gz" "agent-$version.tar.gz" INSTALL

echo "Build complete"
