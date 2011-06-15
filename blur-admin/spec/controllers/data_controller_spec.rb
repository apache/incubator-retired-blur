require "spec_helper"

describe DataController do
  before(:each) do
    @client = mock(Blur::Blur::Client)
    controller.stub!(:thrift_client).and_return(@client)
    controller.stub!(:close_thrift)
    @ability = Ability.new User.new
    @ability.stub!(:can?).and_return(true)
    controller.stub!(:current_ability).and_return(@ability)
  end

  describe "update" do
    it "enables the table if enable is true" do
      table_descr = Blur::TableDescriptor.new :isEnabled => true
      @client.should_receive(:enableTable).with('a_table').and_return(true)
      @client.should_receive(:describe).with('a_table').and_return(table_descr)
      put :update, :enabled => 'true', :id => "a_table"
      response.should render_template true
    end

    it "disables the table if enable is false"
      #TODO: when disable is uncommented, write test
  end
  
  describe "destroy" do
    it "deletes a table from the list" do
      #TODO: when delete table is uncommented, write test for delete
      @client.should_receive(:tableList).and_return(['table1', 'table2'])
      delete :destroy, :id => 'a_table'
      response.should render_template true
    end
  end
end
