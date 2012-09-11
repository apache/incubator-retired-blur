namespace :spec do
  desc "Rspec with simplecov"
  task :simplecov do
    ENV['COVERAGE'] = 'true'
    Rake::Task["spec"].execute
  end
end