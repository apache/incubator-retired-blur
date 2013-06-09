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

describe UserSession do
  before :each do
    activate_authlogic
    @user_session = UserSession.new
  end

  describe "to_key" do
    it "returns nil when it is a new record " do
      @user_session.stub!(:new_record?).and_return(true)
      @user_session.to_key.should be_nil
    end

    it "sends its primary key when it is a new record " do
      @user_session.stub!(:new_record?).and_return(false)
      @user_session.stub_chain(:class, :primary_key).and_return 1
      @user_session.should_receive(:send).with 1
      @user_session.to_key
    end
  end 

  describe "persisted?" do
    it "always returns false" do
      @user_session.persisted?.should be_false
    end
  end 
end