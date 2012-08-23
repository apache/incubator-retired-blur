# This file is copied to spec/ when you run 'rails generate rspec:install'
if ENV["COVERAGE"]
  require 'simplecov'
  SimpleCov.start do 
    ['spec', 'config', 'vendor', 'lib'].each do |root_folder|
      add_filter root_folder
    end
    add_group "Models", "app/models"
    add_group "Controllers", "app/controllers"
    add_group "Helpers", "app/helpers"
  end
end

ENV["RAILS_ENV"] ||= 'test'
require File.expand_path("../../config/environment", __FILE__)
require 'rspec/rails'
require 'capybara/rspec'
require 'capybara/rails'
require 'rspec/autorun'
require 'authlogic/test_case'
require 'cancan/matchers'
include Authlogic::TestCase

Dir[Rails.root.join("spec/support/**/*.rb")].each {|f| require f}

RSpec.configure do |config|
  config.mock_with :rspec
  config.color_enabled = true
  config.tty = true
  config.formatter = :documentation
  config.use_transactional_fixtures = true
end

