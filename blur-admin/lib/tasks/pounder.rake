require 'pounder/pounder'

namespace :pounder do
  desc "Creates a continuous random set of queries into the database"
  task :run => :environment do
    Pounder.run
  end
  
  desc "Clean up pounder data, for use when the pounder fails unexpectedly"
  task :clean => :environment do
    Pounder.clean
  end
end