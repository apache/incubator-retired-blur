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

describe "Blur Queries" do
  before(:each) do
    setup_tests
    #    wait_for_ajax
    #wait_for_dom
  end
  context "Page is loaded" do
    it "should display the correct page elements" do
      visit zookeeper_blur_queries_path(@zookeeper.id)
      page.should have_table('queries-table')
      page.should have_content('User ID')
      page.should have_content('Query')
      page.should have_content('Table Name')
      page.should have_content('Starting Record')
      page.should have_content('Time Submitted')
      page.should have_content('Status')
      page.should have_content('Actions/Info')
      page.should have_css('div', :class => 'range_select')
    end
  end
end