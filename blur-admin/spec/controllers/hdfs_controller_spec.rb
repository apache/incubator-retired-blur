require "spec_helper"

describe HdfsController do
  before(:each) do
    @hdfs_client = mock(Ganapati::Client)
    HdfsThriftClient.stub!(:client).and_return(@hdfs_client)

    @ability = Ability.new User.new
    @ability.stub!(:can?).and_return(true)
    controller.stub!(:current_ability).and_return(@ability)

    @hdfs = Factory.stub :hdfs
    @file_strings = ["hdfs://file-location"]

    @hdfs_stat = mock(ThriftHadoopFileSystem::FileStatus)
  end

  describe "GET index" do
    before do
      Hdfs.stub(:select).and_return([@hdfs])
      @hdfs_client.stub(:ls).and_return(@file_strings)
      @hdfs_client.stub(:exists?).and_return(true)
    end

    it "renders the index template" do
      get :index
      response.should render_template "index"
    end
  end
end