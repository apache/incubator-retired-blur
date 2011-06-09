require "spec_helper"

describe RuntimeController do
  before do
    @client = mock(Blur::Blur::Client)
    controller.stub!(:thrift_client).and_return(@client)
    controller.stub!(:close_thrift)
    @blur_queries = mock_model(BlurQueries)
    #BlurQueries.should_receive(:find_all_by_table_name).and_return([@blur_queries])
    #BlurQueries.should_receive(:all).and_return([@blur_queries])
  end

  describe "show" do
    it "renders the show template when id is a given table" do
      @client.should_receive(:tableList).and_return(['blah'])
      BlurQueries.should_receive(:find_all_by_table_name).and_return([@blur_queries])
      get :show, :id => 'a_table'
      response.should render_template "show"
    end

    it "renders the show template when id is all tables" do
      @client.should_receive(:tableList).and_return(['blah'])
      BlurQueries.should_receive(:all).and_return([@blur_queries])

      get :show, :id => 'all'
      response.should render_template "show"
    end
  end

  describe "update" do
    it "does not cancel a running query if cancel is false" do
      put :update, :cancel => false, :table => 'a_table', :uuid => '1234'
      response.should render_template true
    end

    it "cancels a running query if cancel is true" do
      #TODO: check that canceling query works
      @client.should_receive(:cancelQuery).and_return(true)
      put :update, :cancel => true, :table => 'a_table', :uuid => '1234'
      response.should render_template true
    end
  end
end
