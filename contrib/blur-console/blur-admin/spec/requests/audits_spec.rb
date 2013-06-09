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

describe "Audits" do
  before(:each) do
    setup_tests
    visit audits_path
  end
  context "Page is loaded" do
    it "should show the audits table" do
      page.should have_css('div', :id => 'audits_table')
    end
    it "should have the correct table headers" do
      page.should have_content('Action Taken')
      page.should have_content('Username')
      page.should have_content('User')
      page.should have_content('Zookeeper/Root Path')
      page.should have_content('Model Affected')
      page.should have_content('Mutation Type')
      page.should have_content('Date')
    end
  end
end
