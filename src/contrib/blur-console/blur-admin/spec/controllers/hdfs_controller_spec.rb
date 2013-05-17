require "spec_helper"

describe HdfsController do
  describe "actions" do
    before(:each) do
      # Universal setup
      setup_tests

      # Mock out the client connection to the hdfs
      @hdfs_client = mock(HdfsThriftClient::Client)
      HdfsThriftClient.stub!(:client).and_return(@hdfs_client)

      # Create HDFS model
      @hdfs = FactoryGirl.create :hdfs_with_stats
      @hdfs_mult = [@hdfs]
      Hdfs.stub!(:find).and_return(@hdfs)
      Hdfs.stub!(:all).and_return(@hdfs_mult)
      @file_strings = ["hdfs://file-location"]
    end

    describe "GET index" do
      it "renders the index template when the format is html" do
        get :index, :format => :html
        response.should render_template "index"
      end

      it "renders the index template when the format is json" do
        get :index, :format => :json
        response.body.should == @hdfs_mult.to_json(:methods => [:most_recent_stats, :recent_stats])
      end
    end

    describe "GET info" do
      context "Hdfs Stat found" do
        it "should populate the @hdfs_stat var" do
          get :info, :id => @hdfs.id, :format => :html
          assigns(:hdfs_stat).should == @hdfs.hdfs_stats.last
        end

        it "renders the info partial" do
          get :info, :id => @hdfs.id, :format => :html
          response.should render_template :partial => "_info"
        end
      end
      context "Hdfs Stat not found" do
        it "renders the the warning text" do
          @hdfs.stub_chain(:hdfs_stats, :last).and_return(nil)
          get :info, :id => @hdfs.id, :format => :html 
          response.body.should include("Stats for hdfs ##{@hdfs.id} were not found")
        end
      end
    end

    describe "GET folder_info" do
      before(:each) do
        @hdfs_client.stub!(:stat).and_return('path')
      end

      it "assigns the correct instance variables" do
        @hdfs_client.should_receive(:stat).with '/'
        get :folder_info, :id => @hdfs.id, :fs_path => '/', :format => :html
        assigns(:stat).should == 'path'
      end

      it "renders the folder_info template" do
        HdfsThriftClient.should_receive(:client).with("#{@hdfs.host}:#{@hdfs.port}")
        get :folder_info, :id => @hdfs.id, :fs_path => '/', :format => :html
        response.should render_template :partial => "_folder_info"
      end
    end

    describe "GET slow_folder_info" do
      before(:each) do
        @file_stats = [{:length => 1, :isdir => true}, {:length => 2, :isdir => false}]
        @file_stats.each do |stat|
          stat.stub!(:length).and_return(stat[:length])
          stat.stub!(:isdir).and_return(stat[:isdir])
        end
        @hdfs_client.stub!(:ls).and_return(@file_stats)
      end

      it "renders the correct json" do
        @hdfs_client.should_receive(:ls).with('/', true)
        get :slow_folder_info, :id => @hdfs.id, :fs_path => '/', :format => :json
        response.body.should == {:file_size => '3 Bytes', :file_count => 1, :folder_count => 1}.to_json
      end
    end

    describe "GET expand" do
      before(:each) do
        @file_stats = [{:path => '1/2/3', :isdir => true}, {:path => '1/2/3/4', :isdir => false}]
        @file_stats.each do |stat|
          stat.stub!(:path).and_return(stat[:path])
          stat.stub!(:isdir).and_return(stat[:isdir])
        end
        @hdfs_client.stub!(:ls).and_return(@file_stats)
      end

      it "assigns the correct instance variables with no path" do
        @hdfs_client.should_receive(:ls).with('/')
        get :expand, :id => @hdfs.id, :format => :html
        assigns(:hdfs_id).should == @hdfs.id.to_s
        assigns(:path).should == '/'
        assigns(:children).should == [{'name' => '4', 'is_dir' => false}, {'name' => '3', 'is_dir' => true}, ]
      end

      it "assigns the correct instance variables with a given path" do
        @hdfs_client.should_receive(:ls).with('/path/')
        get :expand, :id => @hdfs.id, :fs_path => '/path', :format => :html
        assigns(:hdfs_id).should == @hdfs.id.to_s
        assigns(:path).should == '/path/'
        assigns(:children).should == [{'name' => '4', 'is_dir' => false}, {'name' => '3', 'is_dir' => true}]
      end

      it "renders the expand partial" do
        HdfsThriftClient.should_receive(:client).with("#{@hdfs.host}:#{@hdfs.port}")
        get :expand, :id => @hdfs.id, :format => :html
        response.should render_template :partial => '_expand'
      end
    end

    describe "GET mkdir" do
      before(:each) do
        @hdfs_client.stub!(:mkdirs)
      end

      it "calls correct client method" do
        @hdfs_client.should_receive(:mkdirs).with("/folder/")
        get :mkdir, :id => @hdfs.id, :fs_path => '/', :folder => 'folder', :format => :json
      end

      it "renders an empty json object" do
        HdfsThriftClient.should_receive(:client).with("#{@hdfs.host}:#{@hdfs.port}")
        get :mkdir, :id => @hdfs.id, :fs_path => '/', :folder => 'folder', :format => :json
        response.body.should == {}.to_json
      end

      it "logs an audit event" do
        Audit.should_receive :log_event
        get :mkdir, :id => @hdfs.id, :fs_path => '/', :folder => 'folder', :format => :json
      end
    end

    describe "GET file_info" do
      before(:each) do
        @hdfs_client.stub!(:stat).and_return('stat')
      end

      it "calls correct client method" do
        @hdfs_client.should_receive(:stat).with('/')
        get :file_info, :id => @hdfs.id, :fs_path => '/', :format => :html
      end

      it "renders the correct template and sets the stat variable" do
        HdfsThriftClient.should_receive(:client).with("#{@hdfs.host}:#{@hdfs.port}")
        get :file_info, :id => @hdfs.id, :fs_path => '/', :format => :html
        response.should render_template :partial => '_file_info'
        assigns(:stat).should == 'stat'
      end
    end

    describe "GET move_file" do
      before(:each) do
        @hdfs_client.stub!(:rename)
      end

      it "calls correct client method" do
        @hdfs_client.should_receive(:rename).with('/test', '/folder/test')
        get :move_file, :id => @hdfs.id, :from => '/test', :to => '/folder/', :format => :json
      end

      it "renders a blank json object" do
        HdfsThriftClient.should_receive(:client).with("#{@hdfs.host}:#{@hdfs.port}")
        get :move_file, :id => @hdfs.id, :from => '/test', :to => '/folder/', :format => :json
        response.body.should == {}.to_json
      end

      it "logs an audit event" do
        Audit.should_receive :log_event
        get :move_file, :id => @hdfs.id, :from => '/test', :to => '/folder/', :format => :json
      end
    end

    describe "GET delete_file" do
      before(:each) do
        @hdfs_client.stub!(:delete)
      end

      it "calls correct client method" do
        @hdfs_client.should_receive(:delete).with('/path/', true)
        get :delete_file, :id => @hdfs.id, :path => '/path/', :format => :json
      end

      it "renders a blank json object" do
        HdfsThriftClient.should_receive(:client).with("#{@hdfs.host}:#{@hdfs.port}")
        get :delete_file, :id => @hdfs.id, :path => '/path/', :format => :json
        response.body.should == {}.to_json
      end

      it "logs an audit event" do
        Audit.should_receive :log_event
        get :delete_file, :id => @hdfs.id, :path => '/path/', :format => :json
      end
    end

    describe "GET upload_form" do
      it "renders the upload form template" do
        get :upload_form
        response.should render_template :partial => "_upload_form"
      end
    end

    describe "POST upload" do
      before(:each) do
        @hdfs_client.stub(:put)
      end

      context "All the params are defined" do
        before(:each) do
          @upload = fixture_file_upload(Rails.root + 'spec/fixtures/test.png', 'image/png')
          @path = "biz/bar/foo"
          class << @upload
            attr_reader :tempfile
          end
          class << @upload.tempfile
            attr_accessor :size
          end
        end

        it "accepts a file less than 25Mb in size" do
          @upload.tempfile.size = 50
          @hdfs_client.should_receive(:put).with(@upload.tempfile.path, @path + '/' + @upload.original_filename)
          HdfsThriftClient.should_receive(:client).with("#{@hdfs.host}:#{@hdfs.port}")
          post :upload, :path => @path, :id => 1, :upload => @upload, :format => :html
          response.body.should render_template :partial => "_upload"
        end

        it "rejects a file greater than 25Mb in size" do
          @upload.tempfile.size = 26220000
          post :upload, :path => @path, :id => 1, :upload => @upload, :format => :html
          response.body.should render_template :partial => "_upload"
          assigns(:error).should_not be_blank
        end

        it "logs an audit event" do
          @upload.tempfile.size = 50
          Audit.should_receive :log_event
          post :upload, :path => @path, :id => 1, :upload => @upload, :format => :html
        end
      end

      context "Params are missing" do
        it "sets the error instance variable" do
          HdfsThriftClient.should_not_receive(:client).with("#{@hdfs.host}:#{@hdfs.port}")
          post :upload, :format => :html
          response.should render_template :partial => '_upload'
          assigns(:error).should_not be_blank
        end

        it "raises an exception and sets the error variable" do
          post :upload, :path => 'path', :id => 1, :upload => 'NOT A FILE', :format => :html
          response.body.should render_template :partial => "_upload"
          assigns(:error).should_not be_blank
        end
      end
    end

    describe "GET file_tree" do
      before(:each) do
        @hdfs_client.stub!(:folder_tree).and_return '{:tree => "branch"}'
      end

      it "assigns the correct instance variables and calls correct client method" do
        @hdfs_client.should_receive(:folder_tree).with('/path/', 4)
        get :file_tree, :id => @hdfs.id, :fs_path => '/path/', :format => :json
      end

      it "renders json" do
        HdfsThriftClient.should_receive(:client).with("#{@hdfs.host}:#{@hdfs.port}")
        get :file_tree, :id => @hdfs.id, :fs_path => '/path/', :format => :json
        response.content_type.should == 'application/json'
      end
    end
  end
end
