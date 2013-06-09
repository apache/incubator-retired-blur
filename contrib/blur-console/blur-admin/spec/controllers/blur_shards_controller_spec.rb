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

describe BlurShardsController do
  describe "actions" do
    before do
      # Universal setup
      setup_tests

      @blur_shard = FactoryGirl.create :blur_shard
      BlurShard.stub!(:find).and_return @blur_shard
    end

    describe 'GET index' do
      before do
        @blur_shards = [@blur_shard]
        BlurShard.stub!(:all).and_return @blur_shards
      end

      it "renders all shards as json" do
        get :index, :format => :json
        response.body.should == @blur_shards.to_json(:except => :cluster_id)
      end
    end

    describe 'DELETE destroy' do
      before do
        @blur_shard.stub!(:destroy)
      end

      it "destroys the shard" do
        @blur_shard.should_receive(:destroy)
        @blur_shard.stub!(:shard_status).and_return 0
        delete :destroy, :id => @blur_shard.id, :format => :json
      end

      it "errors when the shard is enabled" do
        expect {
          @blur_shard.stub!(:shard_status).and_return 1
          delete :destroy, :id => @blur_shard.id, :format => :json
        }.to raise_error
      end

      it "logs the event when the shard is deleted" do
        @blur_shard.stub!(:shard_status).and_return 0
        @blur_shard.stub!(:destroyed?).and_return true
        Audit.should_receive :log_event
        delete :destroy, :id => @blur_shard.id, :format => :json
      end
    end
  end
end
