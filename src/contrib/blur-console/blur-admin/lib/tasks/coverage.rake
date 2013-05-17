tests = [:models, :controllers, :helpers]
tests_tasks = tests.collect{ |type| "cov:#{type.to_s}" }

namespace :cov do
  tests.each do |type|
    desc "Rspec with simplecov (#{type.to_s} only)"
    task type do
      ENV['COVERAGE'] = 'true'
      ENV['PORTION'] = type.to_s
      Rake::Task["spec:#{type}"].execute
    end
  end
end

desc "Rspec with simplecov"
task :cov => tests_tasks