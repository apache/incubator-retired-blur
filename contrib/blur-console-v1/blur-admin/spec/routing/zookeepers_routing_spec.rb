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

describe ZookeepersController do
  describe "routing" do
    it "base and zookeepers routes to #index" do
      get("/").should route_to(:controller => "zookeepers", :action => "index")
      get("/zookeepers").should route_to(:controller => "zookeepers", :action => "index")
    end

    it "index routes to #index as json" do
      get("/zookeepers.json").should route_to(:controller => "zookeepers", :action => "index", :format => 'json')
    end

    it "zookeepers with id routes to #show as json" do
      get("/zookeepers/1.json").should route_to(:controller => "zookeepers", :action => "show", :id => '1', :format => 'json')
    end

    it "delete zookeepers with id routes to #destroy" do
      delete("/zookeepers/1").should route_to(:controller => "zookeepers", :action => "destroy", :id => '1')
    end
  end
end