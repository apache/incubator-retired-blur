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

describe 'User Model' do

  before do
    @user = User.new( :username              => 'bob', 
                      :email                 => 'bob@example.com',
                      :password              => 'password',
                      :password_confirmation => 'password')
  end

  describe 'create a user' do
    it 'is valid with valid parameters' do
      @user.should be_valid
    end

    it 'is invalid with no username' do
      @user.username = nil
      @user.should_not be_valid
    end

    it 'is invalid with no password' do
      @user.password = nil
      @user.password_confirmation = nil
      @user.should_not be_valid
    end
    
    it 'is invalid with no email' do
      @user.email = nil
      @user.should_not be_valid
    end

    it 'is invalid with invalid email' do
      @user.email = 'invalid'
      @user.should_not be_valid
      @user.email = 'invalid@'
      @user.should_not be_valid
    end
    
    
    it 'is invalid with duplicate username' do
      @user.save
      duplicate_user = User.new(:username              => @user.username,
                                :email                 => 'dup_user@example.com',
                                :password              => 'dup_password', 
                                :password_confirmation => 'dup_password')
      duplicate_user.should_not be_valid
    end

    it 'is invalid with duplicate email'  do
      @user.save
      duplicate_user = User.new(:username              => 'duplicate_user',
                                :email                 => @user.email,
                                :password              => 'dup_password', 
                                :password_confirmation => 'dup_password')
      duplicate_user.should_not be_valid
    end

    it 'is valid with long username' do
      username = ''
      100.times {username += 'a'}
      @user.username = username
      @user.should be_valid
    end
    
    it 'is valid with email missing dot ending' do
      @user.email = "tester@test"
      @user.should be_valid
    end

    it 'is invalid with short password' do
      password = 'abc'
      @user.password = password
      @user.password_confirmation = password
      @user.should_not be_valid
    end

    it 'is valid with non alpha-numeric characters in password' do
      password = "aA1!,.:' "
      @user.password = password
      @user.password_confirmation = password
      @user.should be_valid
    end
  end

  describe 'ability' do
    it 'should create a new ability when an ability isnt cached' do
      Ability.should_receive(:new).with(@user)
      @user.ability
    end
  end

  describe 'column preference' do
    it 'should return your saved preference when a preference exists' do
      user_with_preference = FactoryGirl.create :user_with_preferences
      user_with_preference.column_preference.should == user_with_preference.preferences.first
    end

    it 'should create a new saved preference when a preference does not exist' do
      user_without_preference = FactoryGirl.create :user
      user_without_preference.column_preference.pref_type.should == 'column'
      user_without_preference.column_preference.value.should == []
    end
  end

  describe 'zookeeper_preference' do
    it 'should return your zookeeper preference when a preference exists' do
      user_with_preferences = FactoryGirl.create :user_with_preferences
      user_with_preferences.zookeeper_preference.should == user_with_preferences.preferences.last
    end

    it 'should create a new zookeeper preference when a preference does not exist' do
      user_without_preference = FactoryGirl.create :user
      user_without_preference.zookeeper_preference.pref_type.should == 'zookeeper'
      user_without_preference.zookeeper_preference.value.should == nil
    end
  end

  describe 'roles' do
    before(:each) do
      @user_role = FactoryGirl.create :user, :roles => []
    end
    it 'roles= should set the roles mask to the mask of the array given' do
      @user_role.roles = %w[editor]
      @user_role.roles_mask = 16
      @user_role.roles = %w[editor admin reader auditor searcher]
      @user_role.roles_mask = 31
    end

    it 'roles should return the array of all valid roles for this user' do
      @user_role.roles = %w[editor]
      @user_role.roles.should == ['editor']
    end

    it 'is? should return true if the user is a specific role and false if not' do
      @user_role.roles = %w[editor]
      @user_role.is?(:editor).should == true
      @user_role.is?('editor').should == true
      @user_role.is?(:admin).should == false
    end

    it 'the dynamically defined methods should return tru if they are that role' do
      @user_role.roles = %w[editor]
      @user_role.editor?.should == true
      @user_role.admin?.should == false
      @user_role.editor.should == true
      @user_role.admin.should == false
    end
  end
end

