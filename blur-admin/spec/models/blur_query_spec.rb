require 'spec_helper'

describe BlurQuery do
  before(:each) do
    @client = double Blur::Blur::Client
    BlurThriftClient.stub(:client).and_return(@client)
    @query = BlurQuery.new
    @table = mock_model BlurTable
  end

  describe "cancel" do
    context "call to client.cancelQuery is successfull" do
      it "should return true" do
        @query.should_receive(:blur_table).and_return(@table)
        @table.should_receive(:table_name).and_return("my_table_name")
        @client.should_receive(:cancelQuery)
        @query.cancel.should be true
      end
    end

    context "call to client.cancelQuery is unsuccessfull" do
      it "should return false" do
        @client.should_receive(:cancelQuery) { raise Exception }
        lambda {@client.cancelQuery}.should raise_exception
      end
    end
  end
end
