require 'spec_helper'

describe BlurQuery do
  before(:each) do
    @client = mock Blur::Blur::Client
    BlurThriftClient.stub(:client).and_return(@client)
    @client.stub :cancelQuery
    @table = FactoryGirl.create :blur_table
    @query = FactoryGirl.create :blur_query
    @zookeeper = FactoryGirl.create :zookeeper
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

  describe 'state string' do
    it 'should return running when the state is 0' do
      @query.state = 0
      @query.state_str.should == "Running"
    end

    it 'should return running when the state is 1' do
      @query.state = 1
      @query.state_str.should == "Interrupted"
    end

    it 'should return running when the state is 2' do
      @query.state = 2
      @query.state_str.should == "Complete"
    end
  end
end
