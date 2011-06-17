require 'spec_helper'

describe BlurQueries do
  before(:each) do
    @client = Blur::Blur::Client
    BlurThriftClient.stub!(:client).and_return(@client)
    @blur_query = BlurQueries.new
  end

  describe "shards" do
    context "call to client.shardServerLayout is successfull" do
      it "return number of shards" do
        shard_list = {:asdf => [], :abcd => []}
        @client.should_receive(:shardServerLayout).and_return shard_list
        shard_list.should_receive(:length).at_least(1).times
        @blur_query.shards.should be shard_list.length
      end
    end

    context "call to client.shardServerLayout is unsuccessfull" do
      it "return nil" do
        @client.should_receive(:shardServerLayout) { raise Exception }
        lambda {@blur_query.shards}.should raise_exception
      end
    end
  end

  describe "cancel" do
    context "call to client.cancelQuery is successfull" do
      it "return true" do
        @client.should_receive(:cancelQuery)
        @blur_query.cancel.should be true
      end
    end

    context "call to client.cancelQuery is unsuccessfull" do
      it "return nil" do
        @client.should_receive(:cancelQuery) { raise Exception }
        lambda {@client.cancelQuery}.should raise_exception
      end
    end
  end
end
