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
