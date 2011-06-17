require "spec_helper"

describe RuntimeController do
  before do
    @client = mock(Blur::Blur::Client)
    BlurThriftClient.stub!(:client).and_return(@client)
    @blur_queries = mock_model(BlurQueries)
    @ability = Ability.new User.new
    @ability.stub!(:can?).and_return(true)
    controller.stub!(:current_ability).and_return(@ability)
  end

  describe "show" do
    it "renders the show template when id is a given table" do
      @client.should_receive(:tableList).and_return(['a_table'])
      BlurQueries.should_receive(:find_all_by_table_name).and_return([@blur_queries])
      get :show, :id => 'a_table'
      response.should render_template "show"
    end

    it "finds and assigns variables when id is a given table" do
      @client.should_receive(:tableList).and_return(['a_table'])
      BlurQueries.should_receive(:find_all_by_table_name).and_return([@blur_queries])
      get :show, :id => 'a_table'
      assigns(:tables).should == ['a_table']
      assigns(:blur_queries).should == [@blur_queries]
    end

    it "renders the show template when id is all tables" do
      @client.should_receive(:tableList).and_return(['a_table'])
      BlurQueries.should_receive(:all).and_return([@blur_queries])
      get :show, :id => 'all'
      response.should render_template "show"
    end

    it "finds and assigns variables when id is all tables" do
      @client.should_receive(:tableList).and_return(['a_table'])
      BlurQueries.should_receive(:all).and_return([@blur_queries])
      get :show, :id => 'all'
      assigns(:tables).should == ['a_table']
      assigns(:blur_queries).should == [@blur_queries]
    end
  end

  describe "update" do
    it "does not cancel a running query if cancel is false" do
      put :update, :cancel => false, :table => 'a_table', :uuid => '1234'
      response.should render_template true
    end

    it "cancels a running query if cancel is true" do
      #TODO: check that canceling query works
      BlurQueries.should_receive(:find_by_table_name_and_uuid).with('a_table', '1234').and_return(@blur_queries)
      @blur_queries.should_receive(:cancel).and_return(:true)
      put :update, :cancel => true, :table => 'a_table', :uuid => '1234'
      response.should render_template true
    end
  end

  describe "info" do
    it "renders the show template" do
      BlurQueries.should_receive(:find_by_uuid).with('123').and_return(@blur_queries)
      get :info, :uuid => '123'
      response.should render_template true
    end

    it "finds and assigns variables" do
      BlurQueries.should_receive(:find_by_uuid).with('123').and_return(@blur_queries)
      get :info, :uuid => '123'
      assigns(:blur_query).should be @blur_queries
    end
  end
end
