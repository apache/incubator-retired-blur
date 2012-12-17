require 'spec_helper'

describe Cluster do
  before(:each) do
    @cluster = FactoryGirl.create :cluster_with_shard
  end

  describe 'as_json' do
    it "should have the can_update, blur_version and shard status in the json" do
      test_json = @cluster.as_json
      test_json.should include("can_update")
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
