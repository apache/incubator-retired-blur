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

require "spec_helper"

describe UsersController do
  describe "routing" do
    it "base routes to #index" do
      get("/users").should route_to(:controller => "users", :action => "index")
    end

    it "put to preferences#update should route to update" do
      put("/users/1/preferences/column").should route_to(:controller => "preferences", :action => "update", :user_id => '1', :pref_type => 'column')
    end

    it "post to the base uri routes to #create" do
      post("/users").should route_to(:controller => "users", :action => "create")
    end

    it "get new should route to #new" do
      get("/users/new").should route_to(:controller => "users", :action => "new")
    end

    it "get edit should route to #edit" do
      get("/users/1/edit").should route_to(:controller => "users", :action => "edit", :id => '1')
    end

    it "get show should route to #show" do
      get("/users/1").should route_to(:controller => "users", :action => "show", :id => '1')
    end

    it "put update should route to #update" do
      put("/users/1").should route_to(:controller => "users", :action => "update", :id => '1')
    end

    it "delete destroy should route to #destroy" do
      delete("/users/1").should route_to(:controller => "users", :action => "destroy", :id => '1')
    end
  end
end