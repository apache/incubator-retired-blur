namespace :spec do
  desc "Rspec with simplecov"
  task :simplecov do
    ENV['COVERAGE'] = 'true'
    ['models', 'controllers', 'helpers'].each do |type|
      ENV['PORTION'] = type
      Rake::Task["spec:#{type}"].execute
    end
  end
end