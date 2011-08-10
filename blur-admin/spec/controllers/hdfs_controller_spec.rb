require "spec_helper"

describe HdfsController do
  before(:each) do
    @hdfs_client = mock(Ganapati::Client)
    HdfsThriftClient.stub!(:client).and_return(@hdfs_client)

    @ability = Ability.new User.new
    @ability.stub!(:can?).and_return(true)
    controller.stub!(:current_ability).and_return(@ability)

    @hdfs = Factory.stub :hdfs
  end

  describe "GET index" do
    it "renders the index template" do
      Hdfs.stub(:select).and_return(@hdfs)
      get :index
    end
  end

  describe "PUT files" do
    it "renders the selected files"
  end

  describe "PUT search" do
    it "renders search results"
  end

end