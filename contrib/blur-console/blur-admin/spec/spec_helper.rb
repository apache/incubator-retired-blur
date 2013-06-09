# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with this
# work for additional information regarding copyright ownership. The ASF
# licenses this file to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

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
  SimpleCov.command_name("Rspec:#{ENV["PORTION"]}")
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

