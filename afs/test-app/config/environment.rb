# Load the rails application
require File.expand_path('../application', __FILE__)

require 'afs'

# Initialize the rails application
AfsTestApp::Application.initialize!

Afs::Impl.autoload(:Local, File.expand_path('../filesystems/afs/impl/local', __FILE__))
Afs::Impl.autoload(:HDFS, File.expand_path('../filesystems/afs/impl/hdfs', __FILE__))