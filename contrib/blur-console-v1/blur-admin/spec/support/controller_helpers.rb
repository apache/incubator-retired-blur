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

# Setup method for the controllers
module ControllerHelpers
  # Setup the universal variables and stubs
  def setup_variables_and_stubs
    # Create a user for the ability filter
    @user = FactoryGirl.create :user_with_preferences
    @ability = Ability.new @user

    # Create a zookeeper for the current_zk calls
    @zookeeper = FactoryGirl.create :zookeeper

    # Allow the user to perform all of the actions
    @ability.stub!(:can?).and_return(true)

    # Stub out auditing in the controllers
    Audit.stub!(:log_event)
  end

  # Stub out the current ability in the application controller
  def set_ability
    controller.stub!(:current_ability).and_return(@ability)
  end

  # Stub out the current user in the application controller
  def set_current_user
    controller.stub!(:current_user).and_return(@user)
  end

  def set_current_zookeeper
    controller.stub!(:current_zookeeper).and_return(@zookeeper)
  end

  # General setup (for most controllers)
  def setup_tests
    setup_variables_and_stubs
    set_ability
    set_current_user
    set_current_zookeeper
  end
end

RSpec.configure do |config|
  config.include ControllerHelpers, :type => :controller
end