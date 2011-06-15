require "spec_helper"

describe DataController do
  before(:each) do
    @client = mock(Blur::Blur::Client)
    controller.stub!(:thrift_client).and_return(@client)
    controller.stub!(:close_thrift)
    @ability = Ability.new User.new
    @ability.stub!(:can?).and_return(true)
    controller.stub!(:current_ability).and_return(@ability)
    
    @blur_tables = mock_model(BlurTables);
    
  end

  describe "show" do
    it "renders the show template" do
      #need to test the new model setup
      BlurTables.should_receive(:all).and_return([@blur_tables])      
      get :show
      response.should render_template "show"
    end
    
    it "checks the assignment of blur_tables" do
      BlurTables.should_receive(:all).and_return([@blur_tables])      
      get :show
      assigns(:blur_tables).should == [@blur_tables]
    end
  end

  describe "update" do
    before do
      @table = double(BlurTables)
    end


    it "enables the table if enable is true" do
      BlurTables.should_receive(:find_by_table_name).with("a_table").and_return(@table)
      @table.should_receive(:enable)
      @table.should_receive(:is_enabled?).and_return(true)
      put :update, :enabled => 'true', :id => "a_table"
      response.should render_template true
    end

    it "disables the table if enable is false" do
      BlurTables.should_receive(:find_by_table_name).with("a_table").and_return(@table)
      @table.should_receive(:disable)
      @table.should_receive(:is_enabled?).and_return(false)
      put :update, :enabled => 'false', :id => "a_table"
      response.should render_template false
    end
  end
  
  describe "destroy" do
    before do
      @table = double(BlurTables)
    end
    it "deletes a table from the list" do
      @table.should_receive(:destroy).with(true)
      BlurTables.should_receive(:find_by_table_name).with("a_table").and_return(@table)
      delete :destroy, :id => 'a_table', :underlying => true
      response.should render_template true
    end
  end
end
