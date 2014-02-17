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

describe Cluster do
  before(:each) do
    @cluster = FactoryGirl.create :cluster_with_shard
  end

  describe 'as_json' do
    it "should have the can_update and cluster_queried in the json when the blur table flag is true" do
      test_json = @cluster.as_json({:blur_tables => true})
      puts @cluster.can_update
      test_json.should include("can_update")
      test_json.should include("cluster_queried")
    end

    it "should have the can_update and cluster_queried in the json when the blur table flag is true" do
      test_json = @cluster.as_json({:blur_tables => false})
      puts @cluster.can_update
      test_json.should include("shard_blur_version")
      test_json.should include("shard_status")
    end
  end

  describe 'shard_version' do
    it 'should return the common blur version when there is a single version' do
      @cluster.shard_version.should == @cluster.blur_shards.first.blur_version
    end

    it 'should return inconsistent when there are multiple blur versions' do
      @incon_cluster = FactoryGirl.create :cluster_with_shards
      @incon_cluster.shard_version.should == "Inconsistent Blur Versions"
    end

    it 'should return no shards message when there arent any versions' do
      @empty_cluster = FactoryGirl.create :cluster
      @empty_cluster.shard_version.should == "No shards in this Cluster!"
    end
  end

  describe 'shard_status' do
    it 'should return the correct ratio of online to offline shards' do
      @test_cluster = FactoryGirl.create :cluster_with_shards_online
      @test_cluster.shard_status.should == "3 | 3"
    end
  end
end
