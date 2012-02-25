require 'spec_helper'

describe HdfsMetricsController do
  before(:each) do
    @ability = Ability.new User.new
    @ability.stub!(:can?).and_return(true)
    controller.stub!(:current_ability).and_return(@ability)
  end

  describe "actions" do
    before(:each) do
      @hdfs_index = FactoryGirl.create_list :hdfs, 2
      Hdfs.stub(:all).and_return(@hdfs_index)
    end

    describe "GET index" do
      it "should set @hdfs_index to Hdfs.All" do
        get :index
        assigns(:hdfs_index).should == @hdfs_index
      end
    end

    describe "PUT disk_cap_usage" do
      before(:each) do 
        @hdfs = FactoryGirl.create :hdfs_with_stats
      end

      it "put disk with only id should return all within last minute" do
        put :disk_cap_usage, :id => @hdfs.id
        assigns(:results).should == @hdfs.hdfs_stats.where("created_at > '#{1.minute.ago}'")
      end
    end
  end
end
