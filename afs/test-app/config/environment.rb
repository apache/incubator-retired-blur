# Load the rails application
require File.expand_path('../application', __FILE__)

require 'afs'

# Initialize the rails application
AfsTestApp::Application.initialize!

load File.dirname(__FILE__) + '/filesystems/afs/impl/local.rb'
load File.dirname(__FILE__) + '/filesystems/afs/impl/hdfs.rb'
