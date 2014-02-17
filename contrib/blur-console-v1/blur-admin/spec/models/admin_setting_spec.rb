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

describe AdminSetting do
  describe "search filter" do
    it "should create a new setting when one doesnt exist" do
      before_count = AdminSetting.all.count
      AdminSetting.search_filter
      before_count.should == AdminSetting.all.count - 1
    end

    it "should return a setting when one does exist" do
      setting = FactoryGirl.create :admin_setting, :setting => "regex_filter"
      AdminSetting.search_filter.should == setting
    end
  end
end