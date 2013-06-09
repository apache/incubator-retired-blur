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

describe Zookeeper do
  before do
    @user = FactoryGirl.create :user_with_preferences
    @ability = Ability.new @user

    # Allow the user to perform all of the actions
    @ability.stub!(:can?).and_return(true)
  end

  describe 'long running queries' do
    before do
      @zookeeper_with_queries = FactoryGirl.create :zookeeper_with_blur_queries
      Zookeeper.stub!(:find).and_return(@zookeeper_with_queries)
      query = @zookeeper_with_queries.blur_queries[rand @zookeeper_with_queries.blur_queries.count]
      query.state = 0
      query.created_at = 5.minutes.ago
      query.save!
    end

    it "should get the long running query" do
      stats = @zookeeper_with_queries.long_running_queries @user
      stats.count.should == 1
      stats[0]["state"] == 0
    end
  end

  describe 'refresh queries' do
    it "should get the queries within the lower range" do
      @zookeeper = FactoryGirl.create :zookeeper
      test = mock('BlurQuery')
      @zookeeper.stub!(:blur_queries).and_return test
      test.should_receive(:where).with(kind_of(String), 14, 4)
      @zookeeper.refresh_queries 14
    end
  end

  describe 'as json' do
    it "should remove the online_ensemble_nodes key and add ensembles" do
      @zookeeper = FactoryGirl.create :zookeeper
      result = @zookeeper.as_json
      result.should_not include("online_ensemble_nodes")
      result.should include("ensemble")
    end
  end

  describe 'dashboard stats' do
    it "should call query with the large sql string" do
      FactoryGirl.create :zookeeper
      result = Zookeeper.dashboard_stats
      result.first.keys.should include("name", "zookeeper_status", "id", "controller_version", "controller_offline_node", "controller_total", "shard_version", "shard_offline_node", "shard_total", "long_running_queries")
    end
  end

  describe 'clusters with query status' do
    it "should return the clusters with tables and their query counts" do
      @zookeeper = FactoryGirl.create :zookeeper_with_blur_queries
      result = @zookeeper.clusters_with_query_status(@user)
      result.each do |cluster|
        cluster.blur_tables.each do |table|
          table.query_count.should_not be_nil
        end
      end
    end
  end
end
