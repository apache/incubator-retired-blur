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
