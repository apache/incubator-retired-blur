require "spec_helper"

describe HdfsController do
  describe "actions" do
    before(:each) do
      @hdfs_client = mock(HdfsThriftClient::Client)
      HdfsThriftClient.stub!(:client).and_return(@hdfs_client)

      @ability = Ability.new User.new
      @ability.stub!(:can?).and_return(true)
      controller.stub!(:current_ability).and_return(@ability)

      @hdfs = FactoryGirl.create :hdfs_with_stats
      Hdfs.stub!(:find).and_return(@hdfs)
      @file_strings = ["hdfs://file-location"]
    end

    describe "GET index" do
      before(:each) do
        Hdfs.stub(:select).and_return([@hdfs])
      end

      it "finds and assigns the instance variable" do
        Hdfs.should_receive(:all).and_return [@hdfs]
        get :index
        assigns(:instances).should == [@hdfs]
      end

      it "renders the index template" do
        get :index
        response.should render_template "index"
      end
    end

    describe "GET info" do
      context "Hdfs Stat found" do
        it "finds and assigns the hdfs variable" do
          Hdfs.should_receive(:find).with(@hdfs.id.to_s)
          get :info, :id => @hdfs.id
          assigns(:hdfs).should == @hdfs.hdfs_stats.last
        end

        it "renders the info partial" do
          get :info, :id => @hdfs.id
          response.should render_template :partial => "_info"
        end
      end
      context "Hdfs Stat not found" do
        it "renders the the warning text" do
          @hdfs.stub_chain(:hdfs_stats, :last).and_return(nil)
          get :info, :id => @hdfs.id
          response.body.should include("Stats for hdfs ##{@hdfs.id} not found")
        end
      end
    end

    describe "GET folder_info" do
      before(:each) do
        @hdfs_client.stub!(:stat).and_return('path')
      end

      it "assigns the correct instance variables" do
        get :folder_info, :id => @hdfs.id, :fs_path => '/'
        assigns(:stat).should == 'path'
        assigns(:path).should == '/'
      end

      it "renders the folder_info template" do
        HdfsThriftClient.should_receive(:client).with("#{@hdfs.host}:#{@hdfs.port}")
        get :folder_info, :id => @hdfs.id, :fs_path => '/'
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

      it "assigns the correct instance variables" do
        @hdfs_client.should_receive(:ls).with('/', true)
        get :slow_folder_info, :id => @hdfs.id, :fs_path => '/'
        assigns(:path).should == '/'
        assigns(:file_size).should == 3
        assigns(:file_count).should == 1
        assigns(:folder_count).should == 1
      end

      it "renders the folder_info template" do
        HdfsThriftClient.should_receive(:client).with("#{@hdfs.host}:#{@hdfs.port}")
        get :slow_folder_info, :id => @hdfs.id, :fs_path => '/'
        response.content_type.should == 'application/json'
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
        get :expand, :id => @hdfs.id
        assigns(:hdfs_id).should == @hdfs.id.to_s
        assigns(:path).should == '/'
        assigns(:children).should == [{'name' => '3', 'is_dir' => true}, {'name' => '4', 'is_dir' => false}]
      end

      it "assigns the correct instance variables with a given path" do
        @hdfs_client.should_receive(:ls).with('/path/')
        get :expand, :id => @hdfs.id, :fs_path => '/path'
        assigns(:hdfs_id).should == @hdfs.id.to_s
        assigns(:path).should == '/path/'
        assigns(:children).should == [{'name' => '3', 'is_dir' => true}, {'name' => '4', 'is_dir' => false}]
      end

      it "renders the expand partial" do
        HdfsThriftClient.should_receive(:client).with("#{@hdfs.host}:#{@hdfs.port}")
        get :expand, :id => @hdfs.id
        response.should render_template :partial => '_expand'
      end
    end

    describe "GET mkdir" do
      before(:each) do
        @hdfs_client.stub!(:mkdirs)
      end

      it "assigns the correct instance variables and calls correct client method" do
        @hdfs_client.should_receive(:mkdirs).with("/folder/")
        get :mkdir, :id => @hdfs.id, :fs_path => '/', :folder => 'folder'
      end

      it "renders nothing" do
        HdfsThriftClient.should_receive(:client).with("#{@hdfs.host}:#{@hdfs.port}")
        get :mkdir, :id => @hdfs.id, :fs_path => '/', :folder => 'folder'
        response.body.should be_blank
      end
    end

    describe "GET file_info" do
      before(:each) do
        @hdfs_client.stub!(:stat).and_return('stat')
      end

      it "assigns the correct instance variables and calls correct client method" do
        @hdfs_client.should_receive(:stat).with('/')
        get :file_info, :id => @hdfs.id, :fs_path => '/'
      end

      it "renders nothing" do
        HdfsThriftClient.should_receive(:client).with("#{@hdfs.host}:#{@hdfs.port}")
        get :file_info, :id => @hdfs.id, :fs_path => '/'
        response.should render_template :partial => '_file_info'
        assigns(:stat).should == 'stat'
      end
    end

    describe "GET move_file" do
      before(:each) do
        @hdfs_client.stub!(:rename)
      end

      it "assigns the correct instance variables and calls correct client method" do
        @hdfs_client.should_receive(:rename).with('/', '/folder/')
        get :move_file, :id => @hdfs.id, :from => '/', :to => '/folder/'
      end

      it "renders nothing" do
        HdfsThriftClient.should_receive(:client).with("#{@hdfs.host}:#{@hdfs.port}")
        get :move_file, :id => @hdfs.id, :from => '/', :to => '/folder/'
        response.body.should be_blank
      end
    end

    describe "GET delete_file" do
      before(:each) do
        @hdfs_client.stub!(:delete)
      end

      it "assigns the correct instance variables and calls correct client method" do
        @hdfs_client.should_receive(:delete).with('/path/', true)
        get :delete_file, :id => @hdfs.id, :path => '/path/'
      end

      it "renders nothing" do
        HdfsThriftClient.should_receive(:client).with("#{@hdfs.host}:#{@hdfs.port}")
        get :delete_file, :id => @hdfs.id, :path => '/path/'
        response.body.should be_blank
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
          post :upload, :path => @path, :id => 1, :upload => @upload
          response.body.should render_template :partial => "_upload"
        end

        it "rejects a file greater than 25Mb in size" do
          @upload.tempfile.size = 26220000
          post :upload, :path => @path, :id => 1, :upload => @upload
          response.body.should render_template :partial => "_upload"
          assigns(:error).should_not be_blank
        end
      end

      context "Params are missing" do
        it "sets the error instance variable" do
          HdfsThriftClient.should_not_receive(:client).with("#{@hdfs.host}:#{@hdfs.port}")
          post :upload
          response.should render_template :partial => '_upload'
          assigns(:error).should_not be_blank
        end

        it "raises an exception and sets the error variable" do
          post :upload, :path => 'path', :id => 1, :upload => 'NOT A FILE'
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
        get :file_tree, :id => @hdfs.id, :fs_path => '/path/'
      end

      it "renders json" do
        HdfsThriftClient.should_receive(:client).with("#{@hdfs.host}:#{@hdfs.port}")
        get :file_tree, :id => @hdfs.id, :fs_path => '/path/'
        response.content_type.should == 'application/json'
      end
    end

    describe "GET stats" do
      it "with only id should return all within last minute" do
        get :stats, :id => @hdfs.id
        assigns(:results).length.should == 1
        response.content_type.should == 'application/json'
      end

      it "with only return the correct properties" do
        get :stats, :id => @hdfs.id
        assigns(:results)[0].attribute_names.should == %w[id created_at present_capacity dfs_used live_nodes dead_nodes under_replicated corrupt_blocks missing_blocks]
        response.content_type.should == 'application/json'
      end

      it "with stat_mins = 2 should return all within last 2 minutes" do
        get :stats, :id => @hdfs.id, :stat_mins => 2
        assigns(:results).length.should == 2
        response.content_type.should == 'application/json'
      end

      it "with stat_id = @hdfs.hdfs_stats[1].id should return the last one" do
        get :stats, :id => @hdfs.id, :stat_id => @hdfs.hdfs_stats[1].id
        assigns(:results).length.should == 1
        response.content_type.should == 'application/json'
      end
    end
  end
end
