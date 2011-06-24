require "spec_helper"

describe BlurQueriesController do
  before do
    @client = mock(Blur::Blur::Client)
    BlurThriftClient.stub!(:client).and_return(@client)
    mock_time = Time.local(2011, 6, 28, 10, 20, 30)
    Time.stub_chain(:zone, :now).and_return(mock_time + 30.seconds)

    @ability = Ability.new User.new
    @ability.stub!(:can?).and_return(true)
    controller.stub!(:current_ability).and_return(@ability)

    @table = mock_model BlurTable, :id => 1
    @query = mock_model BlurQuery, :created_at => mock_time, :table_id => @table.id
  end

  describe "GET index" do

    context "when an HTML request" do
      it "should render the index template" do
        get :index
        response.should render_template "index"
      end
    end

    context "when an XHR (ajax) request" do
      it "should render the _blur_table partial" do
        xhr :get, :index
        response.should render_template(:partial => '_query_table') 
      end
    end

    it "should assign @blur_tables to be the collection of all blur tables" do
      BlurTable.should_receive(:all).and_return [@table]
      get :index
      assigns(:blur_tables).should == [@table]
    end

    it "should assign @blur_queries to be the collection of blur queries" do
      BlurQuery.stub(:where).and_return([@query])
      get :index
      assigns(:blur_queries).should == [@query]
    end

    it "filters blur queries to within the past minute if no time params given" do
      BlurQuery.should_receive(:where).with(:created_at => Time.zone.now - 1.minutes .. Time.zone.now)
      get :index
    end

    it "filters blur queries to within a specified time if given a time parameter" do
      BlurQuery.should_receive(:where).with :created_at => Time.zone.now - 60.minutes .. Time.zone.now
      get :index, :time => '60'
    end

    it "filters blur queries by super query status if given a super_query_on parameter" do
      BlurQuery.should_receive(:where).with(:super_query_on => 'true', :created_at => Time.zone.now - 1.minutes .. Time.zone.now)
      get :index, :super_query_on => 'true'
    end

    it "filters blur queries by table if given a blur_table_id parameter" do
      BlurQuery.should_receive(:where).with(:blur_table_id => '1', :created_at => Time.zone.now - 1.minutes .. Time.zone.now)
      get :index, :blur_table_id => '1'
    end
  end

  describe "PUT update" do

    it "should assign @blur_query to be the blur query specified by the id parameter" do
      BlurQuery.should_receive(:find).with('1').and_return(@query)
      put :update, :id => '1'
      assigns(:blur_query).should == @query
    end

    it "should not cancel a running query if cancel param is false" do
      BlurQuery.should_receive(:find).with('1').and_return(@query)
      @query.should_not_receive(:cancel)
      put :update, :cancel => 'false', :id => '1'
    end

    it "should cancel a running query if cancel param is true" do
      BlurQuery.should_receive(:find).with('1').and_return(@query)
      @query.should_receive(:cancel)
      put :update, :cancel => 'true', :id => '1'
    end
  end

  describe "GET more_info" do
    it "should render the more_info partial" do
      BlurQuery.stub(:find)
      get :more_info, :id => '1'
      response.should render_template(:partial => '_more_info')
    end

    it "should assign @blur_query to be the blur query specified by the id parameter" do
      BlurQuery.should_receive(:find).with('1').and_return(@query)
      get :more_info, :id => '1'
      assigns(:blur_query).should == @query
    end
  end
end
