require 'spec_helper'

describe Zookeeper do
  before do
    @user = FactoryGirl.create :user_with_preferences
    @ability = Ability.new @user

    # Allow the user to perform all of the actions
    @ability.stub!(:can?).and_return(true)
  end

  describe 'long running queries' do
    before do
      @zookeeper_with_queries = FactoryGirl.create :zookeeper_with_blur_queries
      Zookeeper.stub!(:find).and_return(@zookeeper_with_queries)
      query = @zookeeper_with_queries.blur_queries[rand @zookeeper_with_queries.blur_queries.count]
      query.state = 0
      query.created_at = 5.minutes.ago
      query.save!
    end

    it "should get the long running query" do
      stats = @zookeeper_with_queries.long_running_queries @user
      stats.count.should == 1
      stats[0]["state"] == 0
    end
  end

  describe 'refresh queries' do
    it "should get the queries within the lower range" do
      @zookeeper = FactoryGirl.create :zookeeper
      test = mock('BlurQuery')
      @zookeeper.stub!(:blur_queries).and_return test
      test.should_receive(:where).with(kind_of(String), 14, 4)
      @zookeeper.refresh_queries 14
    end
  end
end
