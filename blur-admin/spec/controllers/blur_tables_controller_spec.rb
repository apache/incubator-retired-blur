require "spec_helper"

describe BlurTablesController do
  describe "actions" do
    before(:each) do
      # Uninversal Setup
      setup_tests

      # Models used for model chain
      @client = mock(Blur::Blur::Client)
      @blur_table = FactoryGirl.create :blur_table
      @clusters = FactoryGirl.create_list :cluster, 3

      # Setup the chain
      @zookeeper.stub!(:clusters_with_query_status).and_return @clusters
      controller.stub!(:thrift_client).and_return(@client)
    end

    describe "GET index" do
      it "should call the before_filter on index" do
        controller.should_receive :zookeepers
        get :index, :format => :html
      end

      it "should assign @clusters to be the current zookeeper's clusters" do
        get :index, :format => :html
        assigns(:clusters).should == @clusters
      end

      it "should call can_update on each cluster" do
        @zookeeper.should_receive(:clusters_with_query_status).with(@user)
        get :index, :format => :html
      end

      it "should render the index template" do
        get :index, :format => :html
        response.should render_template "index"
      end

      it "should render the clusters as json" do
        get :index, :format => :json
        response.body.should == @clusters.to_json(:methods => [:blur_tables], :blur_tables => true)
      end
    end

    describe "PUT enable" do
      before(:each) do
        @tables = [1, 2, 3]
        BlurTable.stub(:find).and_return [@blur_table]
        @blur_table.stub!(:enable)
        @blur_table.stub!(:save)
      end

      it "should enable and update all the given tables" do
        BlurTable.should_receive(:find).with @tables.collect{|id| id.to_s}
        @blur_table.should_receive(:enable)
        @blur_table.should_receive(:table_status=)
        @blur_table.should_receive(:save)
        put :enable, :tables => @tables, :format => :json
      end

      it "should log an audit for every table enabled" do
        Audit.should_receive(:log_event)
        put :enable, :tables => @tables, :format => :json
      end
    end

    describe "PUT disable" do
      before(:each) do
        @tables = [1, 2, 3]
        BlurTable.stub(:find).and_return [@blur_table]
        BlurTable.stub!(:disable)
        @blur_table.stub!(:save)
      end

      it "should disable and update all the given tables" do
        BlurTable.should_receive(:find).with @tables.collect{|id| id.to_s}
        @blur_table.should_receive(:disable)
        @blur_table.should_receive(:table_status=)
        @blur_table.should_receive(:save)
        put :disable, :tables => @tables, :format => :json
      end

      it "should log an audit for every table disabled" do
        Audit.should_receive(:log_event)
        put :disable, :tables => @tables, :format => :json
      end
    end

    describe "DELETE destroy" do
      before(:each) do
        @tables = [1, 2, 3]
        BlurTable.stub(:find).and_return [@blur_table]
        @blur_table.stub!(:blur_destroy)
        BlurTable.stub!(:destroy)
        @blur_table.stub!(:save)
      end

      it "should destroy all the given tables" do
        @blur_table.should_receive(:blur_destroy)
        delete :destroy, :tables => @tables, :format => :json
      end

      it "should set destroy index to true when the param is true" do
        @blur_table.should_receive(:blur_destroy).at_least(:once).with(true, kind_of(String))
        delete :destroy, :tables => @tables, :delete_index => 'true', :format => :json
      end

      it "should set destroy index to false when the param is not true" do
        @blur_table.should_receive(:blur_destroy).at_least(:once).with(false, kind_of(String))
        delete :destroy, :tables => @tables, :delete_index => 'not true', :format => :json
      end

      it "should log an audit for every table destroyed" do
        Audit.should_receive(:log_event)
        put :destroy, :tables => @tables, :format => :json
      end

      it "should forget all the given tables" do
        BlurTable.should_receive(:destroy).with @tables.collect{|id| id.to_s}
        delete :destroy, :tables => @tables, :format => :json
      end
    end

    describe "GET terms" do
      before :each do
        BlurTable.stub(:find).and_return @blur_table
        @blur_table.stub(:terms)
      end

      it "should render a json" do
        get :terms, :format => :json, :id => @blur_table.id
        response.content_type.should == 'application/json'
      end
    end

    describe "GET hosts" do
      it "should render the hosts as json" do
        get :hosts, :format => :json, :id => @blur_table.id
        response.body.should == @blur_table.hosts.to_json()
      end
    end

    describe "GET schema" do
      it "should render the schema as json" do
        get :schema, :format => :json, :id => @blur_table.id
        response.body.should == @blur_table.schema.to_json()
      end
    end

    describe "PUT comment" do
      before :each do
        BlurTable.stub(:find).and_return @blur_table
        @blur_table.stub(:save)
      end

      it "should change the comments in table" do
        put :comment, :id => @blur_table.id, :comment => 'a comment', :format => :json
        @blur_table.comments.should == 'a comment'
      end
    end
  end
end
