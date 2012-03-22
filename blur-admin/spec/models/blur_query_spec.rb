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

    it 'should return Interrupted when the state is 1' do
      @query.state = 1
      @query.state_str.should == "Interrupted"
    end

    it 'should return complete when the state is 2' do
      @query.state = 2
      @query.state_str.should == "Complete"
    end

    it 'should return nil when the state is anyhting else' do
      @query.state = 3
      @query.state_str.should == nil
    end
  end

  describe 'complete' do
    it 'should return 0 when there arent any shards' do 
      @query.total_shards = 0
      @query.complete.should == 0
    end

    it 'should return the number of complete over total when there are shards working' do 
      @query.total_shards = 4
      @query.complete_shards = 2
      @query.complete.should == 0.5
    end
  end

  describe 'summary' do
    it 'should hide the query when the user does not have the proper privileges' do 
      @user = FactoryGirl.create :user, :roles => ['reader']
      @query.summary(@user)[:query].should be_nil
    end

    it 'should show the query when the user has the proper privileges' do 
      @user = FactoryGirl.create :user
      @query.summary(@user)[:query].should == @query.query_string
    end

    context 'summary_state' do
      before(:each) do
        @user = FactoryGirl.create :user
      end

      it 'should return the percent complete if the state is 0' do
        @query.state = 0
        @query.total_shards = 4
        @query.complete_shards = 2
        @query.summary(@user)[:status].should == '50%'
      end

      it 'should return the percent complete and interrupted if the state is 1' do
        @query.state = 1
        @query.total_shards = 4
        @query.complete_shards = 2
        @query.summary(@user)[:status].should == '(Interrupted) - 50%'
      end

      it 'should return complete if the state is 2' do
        @query.state = 2
        @query.summary(@user)[:status].should == 'Complete'
      end
    end
  end
end
