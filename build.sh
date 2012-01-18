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
bundle package

find . -name .DS_Store | xargs rm

rm -f vendor/cache/Saikuro*.gem
rm -f vendor/cache/ZenTest*.gem
rm -f vendor/cache/addressable*.gem
rm -f vendor/cache/arrayfields*.gem
rm -f vendor/cache/autotest*.gem
rm -f vendor/cache/barista*.gem
rm -f vendor/cache/capybara*.gem
rm -f vendor/cache/childprocess*.gem
rm -f vendor/cache/chronic*.gem
rm -f vendor/cache/churn*.gem
rm -f vendor/cache/ci_reporter*.gem
rm -f vendor/cache/coffee*.gem
rm -f vendor/cache/coffee*.gem
rm -f vendor/cache/colored*.gem
rm -f vendor/cache/configatron*.gem
rm -f vendor/cache/cover_me*.gem
rm -f vendor/cache/diff*.gem
rm -f vendor/cache/factory_girl*.gem
rm -f vendor/cache/factory_girl_rails*.gem
rm -f vendor/cache/fattr*.gem
rm -f vendor/cache/ffi*.gem
rm -f vendor/cache/flay*.gem
rm -f vendor/cache/flog*.gem
rm -f vendor/cache/git*.gem
rm -f vendor/cache/hashie*.gem
rm -f vendor/cache/hirb*.gem
rm -f vendor/cache/jeweler*.gem
rm -f vendor/cache/json_pure*.gem
rm -f vendor/cache/launchy*.gem
rm -f vendor/cache/main*.gem
rm -f vendor/cache/map*.gem
rm -f vendor/cache/metric_fu*.gem
rm -f vendor/cache/multi_json*.gem
rm -f vendor/cache/nokogiri*.gem
rm -f vendor/cache/progressbar*.gem
rm -f vendor/cache/rack*.gem
rm -f vendor/cache/rails*.gem
rm -f vendor/cache/rails_best_practices*.gem
rm -f vendor/cache/rcov*.gem
rm -f vendor/cache/reek*.gem
rm -f vendor/cache/roodi*.gem
rm -f vendor/cache/rspec*.gem
rm -f vendor/cache/rspec*.gem
rm -f vendor/cache/rspec*.gem
rm -f vendor/cache/rspec*.gem
rm -f vendor/cache/rspec*.gem
rm -f vendor/cache/ruby*.gem
rm -f vendor/cache/ruby2ruby*.gem
rm -f vendor/cache/ruby_parser*.gem
rm -f vendor/cache/rubyzip*.gem
rm -f vendor/cache/selenium*.gem
rm -f vendor/cache/sexp_processor*.gem
rm -f vendor/cache/syntax*.gem
rm -f vendor/cache/xpath*.gem
rm -f vendor/cache/yamler*.gem

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
