require "spec_helper"

describe BlurTablesController do
  before(:each) do
    @client = mock(Blur::Blur::Client)
    controller.stub!(:thrift_client).and_return(@client)
    controller.stub!(:close_thrift)
    @ability = Ability.new User.new
    @ability.stub!(:can?).and_return(true)
    controller.stub!(:current_ability).and_return(@ability)

    @blur_table = Factory.stub :blur_table
  end

  describe "GET index" do
    before do
      # Set up association chain
      @zookeeper  = Factory.stub :zookeeper

      @zookeeper.stub(:blur_tables).and_return [@blur_table]
      @blur_table.stub(:zookeeper).and_return @zookeeper

      # ApplicationController.current_zookeeper
      Zookeeper.stub(:find_by_id).and_return(nil)
      Zookeeper.stub(:first).and_return @zookeeper
      # ApplicationController.zookeepers
      Zookeeper.stub(:all).and_return [@zookeeper]

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
    
    it "should assign @blur_tables to be the current zookeeper's blur_tables" do
      @zookeeper.should_receive(:blur_tables)
      get :index
      assigns(:blur_tables).should == [@blur_table]
    end
  end

  describe "GET schema" do
    before do
      BlurTable.stub(:find).and_return @blur_table
    end
    it "should assign @blur_table to be the blur table being whose schema is requested" do
      BlurTable.should_receive(:find).with @blur_table.id
      put :update, :id => @blur_table.id
      assigns(:blur_table).should == @blur_table
    end
    it "should render the schema partial" do
      get :schema, :id => @blur_table.id
      response.should render_template :partial => "_schema"
    end
    describe "when an XHR request (ajax)" do
      it "should render the schema partial" do
        xhr :get, :schema, :id => @blur_table.id
        response.should render_template :partial => "_schema"
      end
    end
  end

  describe "GET hosts" do
    before do
      BlurTable.stub(:find).and_return @blur_table
    end
    it "should assign @blur_table to be the blur table being whose hosts is requested" do
      BlurTable.should_receive(:find).with @blur_table.id
      put :update, :id => @blur_table.id
      assigns(:blur_table).should == @blur_table
    end
    it "should render the hosts partial" do
      get :hosts, :id => @blur_table.id
      response.should render_template :partial => "_hosts"
    end
    describe "when an XHR request (ajax)" do
      it "should render the hosts partial" do
        xhr :get, :hosts, :id => @blur_table.id
        response.should render_template :partial => "_hosts"
      end
    end
  end

  describe "PUT update" do
    before do
      BlurTable.stub(:find).and_return(@blur_table)
    end

    it "should assign @blur_table to be the blur table being updated" do
      BlurTable.should_receive(:find).with @blur_table.id
      put :update, :id => @blur_table.id
      assigns(:blur_table).should == @blur_table
    end

    it "enables the table if enable is true" do
      @blur_table.should_receive(:enable)
      put :update, :enable => 'true', :id => @blur_table.id
    end

    it "disables the table if disable is true" do
      @blur_table.should_receive(:disable).and_return(true)
      put :update, :disable => 'true', :id => @blur_table.id
    end

    it "renders the _blur_query partial" do
      put :update, :id => @blur_table.id
      response.should render_template 'blur_table'
    end
  end
  
  describe "DELETE destroy" do
    before do
      BlurTable.stub(:find).and_return(@blur_table)
    end

    it "should assign @blur_table to be the blur table being deleted" do
      BlurTable.should_receive(:find).with(@blur_table.id)
      @blur_table.stub(:destroy)
      delete :destroy, :id => @blur_table.id
      assigns(:blur_table).should == @blur_table
    end

    it "should delete a table and preserve the index" do
      @blur_table.should_receive(:destroy).with(false)
      delete :destroy, :id => @blur_table.id, :delete_index => ''
      response.should render_template nil
    end

    it "should delete a table and the index" do
      @blur_table.should_receive(:destroy).with(true)
      delete :destroy, :id => @blur_table.id, :delete_index => 'true'
      response.should render_template nil
   end
  end
end
