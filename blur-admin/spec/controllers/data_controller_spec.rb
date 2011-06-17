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

    context "when successfull" do

      it "enable the table if enable is true" do
        BlurTables.should_receive(:find_by_table_name).with("a_table").and_return(@table)
        @table.should_receive(:enable).and_return(true)
        put :update, :enabled => 'true', :id => "a_table"
        response.should render_template true
      end

      it "disable the table if enable is false" do
        BlurTables.should_receive(:find_by_table_name).with("a_table").and_return(@table)
        @table.should_receive(:disable).and_return(true)
        put :update, :enabled => 'false', :id => "a_table"
        response.should render_template true
      end
    end

    context "when unsuccessfull" do
      it "attempts to enable the table if enable is true, and renders false" do
        BlurTables.should_receive(:find_by_table_name).with("a_table").and_return(@table)
        @table.should_receive(:enable).and_return(false)
        put :update, :enabled => 'true', :id => "a_table"
        response.should render_template false
      end

      it "attempts to disable the table if enable is false, and renders false" do
        BlurTables.should_receive(:find_by_table_name).with("a_table").and_return(@table)
        @table.should_receive(:disable).and_return(false)
        put :update, :enabled => 'false', :id => "a_table"
        response.should render_template false
      end
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

    it "renders false when the table cannot be deleted" do
      @table.should_receive(:destroy).with(true).and_return false
      BlurTables.should_receive(:find_by_table_name).with("a_table").and_return(@table)
      delete :destroy, :id => 'a_table', :underlying => true
      response.should render_template false
    end
  end
end
