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
bundle exec rake barista:brew

cd ../build
mkdir rails

echo "Copying Rails files"
cp ../blur-admin/Gemfile ../blur-admin/Gemfile.lock ../blur-admin/Rakefile ../blur-admin/config.ru rails

echo "Copying and cleaning app"
cp -r ../blur-admin/app rails
rm -r rails/app/coffeescripts

echo "Copying config"
cp -r ../blur-admin/config rails
rm rails/config/environments/development.rb
rm rails/config/environments/test.rb
rm rails/config/initializers/barista_config.rb

echo "Copying db"
cp -r ../blur-admin/db rails

echo "Copying lib"
cp -r ../blur-admin/lib rails

echo "Copying public"
cp -r ../blur-admin/public rails

echo "Copying script"
cp -r ../blur-admin/script rails

mkdir rails/tmp
touch rails/tmp/placeholder.txt

echo "Copying vendor"
cp -r ../blur-admin/vendor rails

if [ -n "$2" ] && [ $2 = "--certs" ]; then
  echo "Overlaying Cert Auth"
  cp -r ../etc/cert-auth/proof-0.1.0 rails/vendor/gems/
  cp ../etc/cert-auth/certificate-authentication.rb rails/config/initializers/
  cp ../etc/cert-auth/Gemfile rails/
  cp ../etc/cert-auth/production.rb rails/config/environments/
fi

echo "Vendor gems"
cd rails
RAILS_ENV=production bundle package

find rails -name .DS_Store | xargs rm

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
rm agent/lib/junit*.jar
rm agent/lib/hamcrest*.jar

echo "Compressing and zipping agent dir"
mv agent "agent-$version"
tar -cvzf "agent-$version.tar.gz" "agent-$version"

#################################
# Final packaging               #
#################################

cp ../etc/INSTALL .
tar -cvzf "blur-tools-$version.tar.gz" "blur-console-$version.tar.gz" "agent-$version.tar.gz" INSTALL

echo "Build complete"
