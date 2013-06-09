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

describe AdminSettingsController do
  describe "actions" do
    before do
      # Universal setup
      setup_tests
    end

    describe 'PUT update' do
      it 'should create a new admin setting when a previous one DNE' do
        initial_count = AdminSetting.all.length
        put :update, :setting => 'regex'
        AdminSetting.all.length.should == initial_count + 1
      end

      it 'should find an admin setting when a previous one exists' do
        setting = FactoryGirl.create :admin_setting
        initial_count = AdminSetting.all.length
        AdminSetting.should_receive(:find_or_create_by_setting).with('regex').and_return(setting)
        put :update, :setting => 'regex'
        AdminSetting.all.length.should == initial_count
      end

      it 'should update the setting to the given value' do
        setting = FactoryGirl.create :admin_setting
        AdminSetting.should_receive(:find_or_create_by_setting).with('regex').and_return(setting)
        put :update, :setting => 'regex', :value => 'test'
        setting.value.should == 'test'
      end

      it 'should succeed with a given setting' do
        expect {
          put :update, :setting => 'regex'
        }.to_not raise_error
      end

      it 'should fail when a setting is not given' do
        expect {
          put :update
        }.to raise_error
      end
    end
  end
end