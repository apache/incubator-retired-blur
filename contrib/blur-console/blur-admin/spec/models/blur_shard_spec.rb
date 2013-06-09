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

describe BlurShard do
  describe "destroy_parent_cluster" do
    before do
      @cluster = FactoryGirl.create :cluster
      @shard = FactoryGirl.create :blur_shard
      @shard.stub!(:cluster).and_return @cluster
    end

    it "should do nothing when it has siblings" do
      @cluster.stub_chain(:blur_shards, :count).and_return 1
      @shard.destroy
      @cluster.should_not_receive(:destroy)
    end
    it "should destroy the cluster when all the shards are destroyed" do
      @cluster.stub_chain(:blur_shards, :count).and_return 0
      @cluster.should_receive(:destroy)
      @shard.destroy
    end
  end
end
