require 'spec_helper'

describe BlurQuery do
  before(:each) do
    @client = double Blur::Blur::Client
    BlurThriftClient.stub(:client).and_return(@client)
    @client.stub :cancelQuery
    @table = Factory.create :blur_table
    @query = Factory.create :blur_query
    @zookeeper = Factory.create :zookeeper
    @query.blur_table = @table
    @table.stub(:zookeeper).and_return(@zookeeper)
  end

  describe "cancel" do
    context "call to client.cancelQuery is successful" do
      it "should return true" do
        @client.should_receive(:cancelQuery).with(@table.table_name, @query.uuid).and_return nil
        @query.cancel.should be true
      end
    end

    context "call to client.cancelQuery is unsuccessful" do
      it "should return false" do
        @client.should_receive(:cancelQuery) { raise Exception }
        @query.cancel.should be false
      end
    end
  end
end
