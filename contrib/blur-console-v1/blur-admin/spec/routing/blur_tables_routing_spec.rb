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

describe BlurTablesController do
  describe "routing" do
    it "base routes to #index" do
      get("/zookeepers/1/blur_tables").should route_to(:controller => "blur_tables", :action => "index", :zookeeper_id => '1')
    end

    it "json index routes to #index" do
      get("/zookeepers/1/blur_tables.json").should route_to(:controller => "blur_tables", :action => "index", :zookeeper_id => '1', :format => 'json')
    end

    it "enable routes to #enable" do
      put("/zookeepers/1/blur_tables/enable.json").should route_to(:controller => "blur_tables", :action => "enable", :zookeeper_id => '1', :format => 'json')
    end

    it "disable routes to #disable" do
      put("/zookeepers/1/blur_tables/disable.json").should route_to(:controller => "blur_tables", :action => "disable", :zookeeper_id => '1', :format => 'json')
    end

    it "destroy routes to #destroy" do
      delete("/zookeepers/1/blur_tables.json").should route_to(:controller => "blur_tables", :action => "destroy", :zookeeper_id => '1', :format => 'json')
    end

    it "terms routes to #terms" do
      get("/blur_tables/1/terms.json").should route_to(:controller => "blur_tables", :action => "terms", :id => '1', :format => 'json')
    end
  end
end