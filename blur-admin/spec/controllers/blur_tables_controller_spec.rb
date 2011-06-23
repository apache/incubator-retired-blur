require "spec_helper"

describe BlurTablesController do
  before(:each) do
    @client = mock(Blur::Blur::Client)
    controller.stub!(:thrift_client).and_return(@client)
    controller.stub!(:close_thrift)
    @ability = Ability.new User.new
    @ability.stub!(:can?).and_return(true)
    controller.stub!(:current_ability).and_return(@ability)
    @table = mock_model(BlurTable).as_null_object
  end

  describe "GET index" do
    it "should render the index template" do
      #need to test the new model setup
      get :index
      response.should render_template "index"
    end
    
    it "should assign @blur_tables to be the collection of all blur_tables" do
      BlurTable.should_receive(:all).and_return([@table])      
      get :index
      assigns(:blur_tables).should == [@table]
    end
  end

  describe "PUT update" do

    it "should assign @blur_table to be the blur table being updated" do
      BlurTable.should_receive(:find).with('1').and_return(@table)
      put :update, :id => '1'
      assigns(:blur_table).should == @table
    end

    it "enables the table if enable is true" do
      BlurTable.stub(:find).and_return(@table)
      @table.should_receive(:enable)
      put :update, :enable => 'true', :id => '1'
    end

    it "disables the table if disable is true" do
      BlurTable.stub(:find).and_return(@table)
      @table.should_receive(:disable).and_return(true)
      put :update, :disable => 'true', :id => '1'
    end

    it "renders the _blur_query partial" do
      BlurTable.stub(:find)
      put :update, :id => '1'
      response.should render_template 'blur_table'
    end
  end
  
  describe "destroy" do

    it "should assign @blur_table to be the blur table being updated" do
      BlurTable.should_receive(:find).with('1').and_return(@table)
      put :update, :id => '1'
      assigns(:blur_table).should == @table
    end

    it "should delete a table and preserve the index" do
      BlurTable.stub(:find).and_return(@table)
      @table.should_receive(:destroy)
      delete :destroy, :id => '1', :delete_index => ''
      response.should render_template nil
    end

    it "should delete a table and the index" do
      BlurTable.stub(:find).and_return(@table)
      @table.should_receive(:destroy).with(true)
      delete :destroy, :id => '1', :delete_index => 'true'
      response.should render_template nil
   end
  end
end
