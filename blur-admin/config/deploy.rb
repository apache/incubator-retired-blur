$:.unshift(File.expand_path('./lib', ENV['rvm_path'])) # Add RVM's lib directory to the load path.
require "rvm/capistrano"                  # Load RVM's capistrano plugin.
set :rvm_ruby_string, '1.9.2'        # Or whatever env you want it to run in.
set :rvm_type, :user

require 'bundler/capistrano'

set :repository,  "git@scm.nearinfinity.com:blur-tools.git"
set :branch, 'develop'
set :scm, :git

set :application, "blur-admin"
set :deploy_to, "/home/localadmin/blur-tools"

role :web, "nic-blurtest01.nearinfinity.com"                          # Your HTTP server, Apache/etc
role :app, "nic-blurtest01.nearinfinity.com"                          # This may be the same as your `Web` server
role :db,  "nic-blurtest01.nearinfinity.com", :primary => true # This is where Rails migrations will run

set :user, "localadmin"
set :use_sudo, false
set :deploy_subdir, "blur-admin"

# if you're still using the script/reaper helper you will need
# these http://github.com/rails/irs_process_scripts

# If you are using Passenger mod_rails uncomment this:
namespace :deploy do
  task :start do ; end
  task :stop do ; end
  task :restart, :roles => :app, :except => { :no_release => true } do
    run "#{try_sudo} touch #{File.join(current_path,'tmp','restart.txt')}"
  end
end