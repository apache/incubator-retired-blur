#!/bin/bash

blur_tools_version=`cat VERSION`

echo "This script will help you install the components for Blur Tools version $blur_tools_version."
if [ ! -e "$BLUR_TOOLS_HOME" ]; then
	echo "Unable to find BLUR_TOOLS_HOME.  Please specify the location of the home directory where you want Blur Tools installed."
	read home_dir
	if [ -z "$home_dir" ]; then
		home_dir = `pwd`
	fi
	echo "Home directory is going to be set to $home_dir.  To have this part go away in the future please set BLUR_TOOLS_HOME to this directory."
fi

if [ ! -e "$home_dir" ]; then
	mkdir -p "$home_dir"
fi

echo "Installing the web application"
tar xzf "blur-console-$blur_tools_version.tar.gz" -C "$home_dir"

echo "Installing the agent"
tar xzf "agent-$blur_tools_version.tar.gz" -C "$home_dir"

echo "Setting up symlinks"
if [ -e "$home_dir/rails" ]; then
	rm "$home_dir/rails"
fi
ln -s "$home_dir/rails-$blur_tools_version" "$home_dir/rails"

if [ -e "$home_dir/agent" ]; then
	rm "$home_dir/agent"
fi
ln -s "$home_dir/agent-$blur_tools_version" "$home_dir/agent"

if [ ! -e "$home_dir/config" ]; then
	echo "Setting up common config directory"
	mkdir "$home_dir/config"
	
	echo "Move files into place"
	mv "$home_dir/agent/conf/agent.config.sample" "$home_dir/config/blur-agent.config"
	
	echo "Since this is a new installation please update the $home_dir/config/blur-agent.config file with the appropriate values before continuing.  Hit enter to continue:"
	read tmp_input
fi

ln -s "$home_dir/config/blur-agent.config" "$home_dir/agent/conf/blur-agent.config"

echo "Create log directories"
if [ ! -e "$home_dir/agent/log" ]; then
	mkdir "$home_dir/agent/log"
fi
if [ ! -e "$home_dir/rails/log" ]; then
	mkdir "$home_dir/rails/log"
fi

current_dir=`pwd`

echo "Initializing web application.  NOTE: This will update the database"
cd "$home_dir/rails"
bundle install --local
bundle exec rake RAILS_ENV=production db:create
bundle exec rake RAILS_ENV=production db:migrate
bundle exec rake RAILS_ENV=production db:seed
cd "$current_dir"

echo "Ok we are almost finished.  There may be some final things that you will need to do:"
if ls "$home_dir/config/*.lic" > /dev/null 2>&1; then
	echo "    * You will need to add your license file to $home_dir/config and put that path into the blur-agent.config file"
fi

if ls "$home_dir/config/mysql*.jar" > /dev/null 2>&1; then
	echo "    * Download the mysql jdbc connector jar file and put it in $home_dir/config"
fi

echo "    * Run the command 'ln -s $home_dir/config/mysql-version.jar $home_dir/agent/lib/mysql.jar' replacing mysql-version.jar with your actual file name"
echo "    * If not already done setup your webserver to point to $home_dir/rails"
echo "    * The agent can be started by running $home_dir/agent/bin/start-agent.sh"