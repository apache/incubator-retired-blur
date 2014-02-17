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

require 'spec_helper'

describe "login" do
  # generate a valid user
  let(:user) { Factory.create :user }

  before do
    visit login_path
  end

  context "with valid login credentials" do
    before do
      fill_in 'user_session_username', :with => user.username
      fill_in 'user_session_password', :with => user.password
      click_button 'Log In'
    end

    it "should render the dashboard" do
      current_path.should == root_path
    end
  end

  context "with invalid password" do
    before do
      fill_in 'user_session_username', :with => user.username
      fill_in 'user_session_password', :with => 'invalid'
      click_button 'Log In'
    end

    it "should render the new user sessions page with apppropriate errors" do
      current_path.should == user_sessions_path
      page.should have_selector '#error_explanation'
      page.should have_content 'Password is not valid'
    end
  end

  context "with invalid username" do
    before do
      fill_in 'user_session_username', :with => 'invalid'
      fill_in 'user_session_password', :with => user.password
      click_button 'Log In'
    end

    it "should render the new user sessions page with apppropriate errors" do
      current_path.should == user_sessions_path
      page.should have_selector '#error_explanation'
      page.should have_content 'Username is not valid'
    end
  end
  context "with no credentials" do
    before do
      click_button 'Log In'
    end

    it "should render the new user sessions page with apppropriate errors" do
      current_path.should == user_sessions_path
      page.should have_selector '#error_explanation'
      page.should have_content 'You did not provide any details for authentication.'
    end
  end
end
