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

describe UserSessionsController do
  describe "actions" do
    before(:each) do
      # setup the tests
      activate_authlogic
      setup_tests

      # usersessions specific
      @user_session = UserSession.new({:username => @user.username, :password => @user.password, :commit => "Log In"})
      @user_session.stub!(:user).and_return @user
      controller.stub!(:current_user_session).and_return @user_session
    end

    describe "GET 'new'" do
      it "assigns a new user_session as @user_session" do
        UserSession.should_receive(:new).at_least(1).times.and_return(@user_session)
        get :new, :format => :html
        assigns(:user_session).should == (@user_session)
      end

      it "should render new view" do
        get :new, :format => :html
        response.should render_template :new
      end
    end

    describe "POST create" do
      it "assigns a new session to @user_session and saves successfully" do
        UserSession.stub(:new).and_return @user_session
        @user_session.should_receive(:save).and_return true
        post :create, :user_session => {:username => @user.username, :password => @user.password, :commit => "Log In"}, :format => :html
        assigns(:user_session).should be(@user_session)
        response.should redirect_to(root_path)
      end

      it "assigns a new session to @user_session and saves unsuccessfully" do
        UserSession.stub(:new).and_return @user_session
        @user_session.should_receive(:save).and_return false
        post :create, :user_session => {:username => @user.username, :password => @user.password, :commit => "Log In"}, :format => :html
        response.should redirect_to(login_path)
      end
    end

    describe "DELETE 'destroy'" do
      it "finds and destroys current user session" do
        @user_session.should_receive(:destroy)
        delete :destroy, :format => :html
      end

      it "redirects to root_url with notice" do
        delete :destroy, :format => :html
        response.should redirect_to(login_path)
        flash[:notice].should_not be_blank
      end
    end
  end
end
