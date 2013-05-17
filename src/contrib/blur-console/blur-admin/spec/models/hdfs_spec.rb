require 'spec_helper'

describe Hdfs do
  before :each do
    @hdfs = FactoryGirl.create :hdfs
    @hdfs_stat = FactoryGirl.create :hdfs_stat
    @hdfs_stat2 = FactoryGirl.create :hdfs_stat
    @hdfs.stub!(:hdfs_stats).and_return [@hdfs_stat2, @hdfs_stat]
  end

  it 'returns the most recent stats' do
    @hdfs.most_recent_stats.should == @hdfs_stat
  end

  it 'returns true if stats were updated less than 1 minute ago' do
    @hdfs_stat.created_at = Time.now
    @hdfs.recent_stats.should == true
  end

  it 'returns false if stats were not updated in the last minute' do
    @hdfs_stat.created_at = Time.now - 1000*2
    @hdfs.recent_stats.should == false
  end
end
