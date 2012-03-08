namespace :blur do
  desc "Recompiles the stylesheet assets only"
  task :recompile => :environment do
    Rails.application.config.assets.precompile = [/.*\.s?css/]
    Rake::Task["assets:precompile"].invoke
  end
end