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

describe ClustersController do
  describe "actions" do
    before do
      # Universal setup
      setup_tests

      @cluster = FactoryGirl.create :cluster
      Cluster.stub!(:find).and_return @cluster
    end

    describe 'DELETE destroy' do
      before do
        @cluster.stub!(:destroy)
      end

      it "destroys the cluster" do
        @cluster.should_receive(:destroy)
        @cluster.stub!(:cluster_status).and_return 0
        delete :destroy, :id => @cluster.id, :format => :json
      end

      it "errors when the cluster is enabled" do
        expect {
          @cluster.stub!(:cluster_status).and_return 1
          delete :destroy, :id => @cluster.id, :format => :json
        }.to raise_error
      end

      it "logs the event when the cluster is deleted" do
        @cluster.stub!(:cluster_status).and_return 0
        @cluster.stub!(:destroyed?).and_return true
        Audit.should_receive :log_event
        delete :destroy, :id => @cluster.id, :format => :json
      end
    end
  end
end
