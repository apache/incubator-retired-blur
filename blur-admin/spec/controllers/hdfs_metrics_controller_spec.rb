require 'spec_helper'

describe HdfsMetricsController do
  describe "actions" do
    before(:each) do
      @ability = Ability.new User.new
      @ability.stub!(:can?).and_return(true)
      controller.stub!(:current_ability).and_return(@ability)
      @hdfs_index = FactoryGirl.create_list :hdfs, 2
      Hdfs.stub(:all).and_return(@hdfs_index)
    end

    describe "GET index" do
      it "should set @hdfs_index to Hdfs.All" do
        get :index
        assigns(:hdfs_index).should == @hdfs_index
        response.should render_template :index
      end
    end
  end
end
