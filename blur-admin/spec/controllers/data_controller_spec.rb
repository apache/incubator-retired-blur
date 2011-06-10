require "spec_helper"

describe DataController do
  before (:each) do
    @client = mock(Blur::Blur::Client)
    controller.stub!(:thrift_client).and_return(@client)
    controller.stub!(:close_thrift)
  end

  describe "show" do
    it "renders the show template" do
      bq = Blur::BlurQuery.new :queryStr => '*', :fetch => 1, :superQueryOn => false
      @client.should_receive(:tableList).and_return(['blah'])
      @client.should_receive(:describe).with('blah').and_return(Blur::TableDescriptor.new)
      @client.should_receive(:schema).with('blah').and_return(Blur::Schema.new)
      @client.should_receive(:shardServerLayout).with('blah').and_return(Blur::Blur::ShardServerLayout_result.new)
      @client.should_receive(:query).with('blah', bq).and_return(Blur::BlurResults.new)
      get :show
      response.should render_template "show"
    end
  end

  describe "update" do
    it "enables the table if enable is true" do
      table_descr = Blur::TableDescriptor.new :isEnabled => true
      @client.should_receive(:enableTable).with('a_table').and_return(true)
      @client.should_receive(:describe).with('a_table').and_return(table_descr)
      put :update, :enabled => 'true', :id => "a_table"
      response.should render_template true
    end

    it "disbales the table if enable is false"
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
