require 'spec_helper'

describe HdfsStat do
  before(:each) do
    @stat = FactoryGirl.create :hdfs_stat
  end

  it 'capacity should return the capacity in gigabytes' do
    @stat.present_capacity = 3 * 1024**3
    @stat.capacity.should == 3
    @stat.capacity.kind_of? Float
  end

  it 'capacity should return the capacity in gigabytes' do
    @stat.dfs_used_real = 3 * 1024**3
    @stat.used.should == 3
    @stat.capacity.kind_of? Float 
  end
end
