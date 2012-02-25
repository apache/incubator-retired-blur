# This file is copied to spec/ when you run 'rails generate rspec:install'
require 'cover_me'
ENV["RAILS_ENV"] ||= 'test'
require File.expand_path("../../config/environment", __FILE__)
require 'rspec/rails'
require 'authlogic/test_case'
require 'capybara/rspec'
require 'capybara/rails'
require "cancan/matchers"
include Authlogic::TestCase

Dir[Rails.root.join("spec/support/**/*.rb")].each {|f| require f}

# Requires supporting ruby files with custom matchers and macros, etc,
# in spec/support/ and its subdirectories.
Dir[Rails.root.join("spec/support/**/*.rb")].each {|f| require f}

RSpec.configure do |config|
  config.mock_with :rspec
  config.color_enabled = true
  config.tty = true
  config.formatter = :documentation
  config.use_transactional_fixtures = true
end
