#!/bin/bash

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
bundle exec rake sass:build

cd ../build
mkdir rails

echo "Copying Rails files"
cp ../blur-admin/Gemfile ../blur-admin/Gemfile.lock ../blur-admin/Rakefile ../blur-admin/config.ru rails

echo "Copying and cleaning app"
cp -r ../blur-admin/app rails
rm -r rails/app/coffeescripts
rm -r rails/app/stylesheets

echo "Copying config"
cp -r ../blur-admin/config rails
rm rails/config/environments/development.rb
rm rails/config/environments/test.rb

echo "Copying db"
cp -r ../blur-admin/db rails

echo "Copying lib"
cp -r ../blur-admin/lib rails

echo "Copying public"
cp -r ../blur-admin/public rails

echo "Copying script"
cp -r ../blur-admin/script rails

mkdir rails/tmp

echo "Copying vendor"
cp -r ../blur-admin/vendor rails

if [ $1 = "--certs" ]; then
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
tar -cvzf blur.tar.gz rails/*

#################################
# Prep Agent                    #
#################################

echo "Build agent jar"
cd ../blur-agent
mvn clean package -DskipTests
cd ../build

echo "Copy jar"
mkdir agent
cp ../blur-agent/target/blur-agent-*-jar-with-dependencies.jar agent/agent.jar

echo "Copy extra pieces"
cp ../etc/agent.config.sample agent
cp ../etc/agent.sh agent

echo "Compressing and zipping agent dir"
tar -cvzf agent.tar.gz agent/*

#################################
# Final packaging               #
#################################

cp ../etc/INSTALL .
tar -cvzf blur-tools.tar.gz blur.tar.gz agent.tar.gz INSTALL

echo "Build complete"
