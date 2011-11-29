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
  describe "GET upload_form" do
    it "renders the upload form template" do
      get :upload_form
      response.should render_template "upload_form"
    end  
  end
  
  describe "POST upload" do
    render_views
    before do
      Hdfs.stub(:find).and_return([@hdfs])
      @hdfs.stub(:host).and_return("foo")
      @hdfs.stub(:port).and_return(5000)
      @hdfs_client.stub(:put).and_return(true)
    end
    it "accepts a file less than 25Mb in size" do
      @uploadfile = mock(ActionDispatch::Http::UploadedFile)
      @tempfile = mock(File)
      @uploadfile.stub(:original_filename).and_return("test.png")
      @uploadfile.stub(:tempfile).and_return(@tempfile)
      @tempfile.stub(:path).and_return("foo/bar/biz")
      @tempfile.stub(:size).and_return(500)
      @path = "biz/bar/foo"
      post :upload, {:path => @path,:hdfs_id => 1, :upload => @uploadfile}
      response.body.should render_template "upload"
      
    end
    
    it "rejects a file greater than 25Mb in size" do
      
    end
  end
end
