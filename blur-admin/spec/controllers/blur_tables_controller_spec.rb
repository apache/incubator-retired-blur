require "spec_helper"

describe BlurTablesController do
  describe "actions" do
    before(:each) do
      # Uninversal Setup
      setup_tests

      # Models used for model chain
      @zookeeper  = FactoryGirl.create :zookeeper
      @client = mock(Blur::Blur::Client)
      @blur_table = FactoryGirl.create :blur_table
      @cluster = FactoryGirl.create_list :cluster, 3

      # Setup the chain
      @zookeeper.stub_chain(:blur_tables, :order).and_return [@blur_table]
      @zookeeper.stub_chain(:clusters, :order).and_return @cluster
      controller.stub!(:thrift_client).and_return(@client)
      controller.stub!(:current_ability).and_return(@ability)
      Zookeeper.stub!(:find_by_id).and_return(@zookeeper)
      Zookeeper.stub!(:first).and_return(@zookeeper)
    end

    describe "GET index" do
      before(:each) do
        Zookeeper.stub!(:order).and_return [@zookeeper]
      end

      it "should assign @zookeepers to be the collection of all zookeepers" do
        get :index
        assigns(:zookeepers).should == [@zookeeper]
      end

      it "should assign @current_zookeeper" do
        get :index
        assigns(:current_zookeeper).should == @zookeeper
      end

      it "should render the index template" do
        #need to test the new model setup
        get :index
        response.should render_template "index"
      end

      it "should assign @clusters to be the current zookeeper's clusters" do
        @zookeeper.should_receive(:clusters)
        get :index
        assigns(:clusters).should == @cluster
      end
    end

    describe "PUT enable" do
      before(:each) do
        @tables = [1, 2, 3]
        BlurTable.stub(:find).and_return @blur_table
        BlurTable.stub!(:enable)
        @blur_table.stub!(:save)
      end

      it "should enable all the given tables" do
        @tables.each do |id|
          BlurTable.should_receive(:find).with(id.to_s)
        end
        @blur_table.should_receive(:enable).exactly(@tables.length).times
        put :enable, :tables => @tables
      end

      it "should log an audit for every table enabled" do
        Audit.should_receive(:log_event).exactly(@tables.length).times
        put :enable, :tables => @tables
      end
    end

    describe "PUT disable" do
      before(:each) do
        @tables = [1, 2, 3]
        BlurTable.stub(:find).and_return @blur_table
        BlurTable.stub!(:disable)
        @blur_table.stub!(:save)
      end

      it "should disable all the given tables" do
        @tables.each do |id|
          BlurTable.should_receive(:find).with(id.to_s)
        end
        @blur_table.should_receive(:disable).exactly(@tables.length).times
        put :disable, :tables => @tables
      end

      it "should log an audit for every table disabled" do
        Audit.should_receive(:log_event).exactly(@tables.length).times
        put :disable, :tables => @tables
      end
    end

    describe "DELETE destroy" do
      before(:each) do
        @tables = [1, 2, 3]
        BlurTable.stub(:find).and_return @blur_table
        BlurTable.stub!(:blur_destroy)
        BlurTable.stub!(:destroy)
        @blur_table.stub!(:save)
      end

      it "should destroy all the given tables" do
        @tables.each do |id|
          BlurTable.should_receive(:find).with(id.to_s)
        end
        @blur_table.should_receive(:blur_destroy).exactly(@tables.length).times
        delete :destroy, :tables => @tables
      end

      it "should set destroy index to true when the param is true" do
        @blur_table.should_receive(:blur_destroy).at_least(:once).with(true, kind_of(String))
        delete :destroy, :tables => @tables, :delete_index => 'true'
      end

      it "should set destroy index to false when the param is not true" do
        @blur_table.should_receive(:blur_destroy).at_least(:once).with(false, kind_of(String))
        delete :destroy, :tables => @tables, :delete_index => 'not true'
      end

      it "should log an audit for every table destroyed" do
        Audit.should_receive(:log_event).exactly(@tables.length).times
        put :destroy, :tables => @tables
      end

      it "should forget all the given tables" do
        BlurTable.should_receive(:destroy).with(@blur_table)
        delete :destroy, :tables => @tables
      end
    end

    describe "GET terms" do
      before :each do
        BlurTable.stub(:find).and_return @blur_table
        @blur_table.stub(:terms)
      end

      it "should render a json" do
        get :terms
        response.content_type.should == 'application/json'
      end
    end

    describe "PUT comment" do
      before :each do
        BlurTable.stub(:find).and_return @blur_table
        @blur_table.stub(:save)
      end

      it "should change the comments in table" do
        put :comment, :input => 'a comment'
        @blur_table.comments.should == 'a comment'
      end
    end
  end
end
