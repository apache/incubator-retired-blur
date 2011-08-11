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

  describe "PUT files" do
    before do
      Hdfs.stub(:find).and_return(@hdfs)
    end

    it "renders the selected files" do
      @hdfs_client.should_receive(:stat).and_return(@hdfs_stat)
      @hdfs_client.should_receive(:exists?).and_return(true)
      @hdfs_client.should_receive(:ls).and_return(@file_strings)
      get :files, :connection => '4', :file => "hdfs://file-location"
      response.should render_template "files"
    end
  end

  describe "PUT search" do
    before do
      Hdfs.stub(:find).and_return(@hdfs)
    end
    it "renders search results" do
      @hdfs_client.stub(:stat).and_return(@hdfs_stat)
      @hdfs_client.should_receive(:exists?).and_return(true)
      @hdfs_client.should_receive(:ls).and_return(@file_strings)
      get :files, :results => { "hdfs://file-location" => '4' }
    end
  end

end