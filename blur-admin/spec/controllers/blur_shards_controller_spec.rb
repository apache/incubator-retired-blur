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
        @blur_shard.stub!(:status).and_return 0
        delete :destroy, :id => @blur_shard.id, :format => :json
      end

      it "errors when the shard is enabled" do
        expect {
          @blur_shard.stub!(:status).and_return 1
          delete :destroy, :id => @blur_shard.id, :format => :json
        }.to raise_error
      end

      it "logs the event when the shard is deleted" do
        @blur_shard.stub!(:status).and_return 0
        @blur_shard.stub!(:destroyed?).and_return true
        Audit.should_receive :log_event
        delete :destroy, :id => @blur_shard.id, :format => :json
      end
    end
  end
end
