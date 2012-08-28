require "spec_helper"

describe BlurQueriesController do
  describe "actions" do
    before do
      # Universal Setup
      setup_tests

      # Mock out the blur client
      @client = mock(Blur::Blur::Client)
      BlurThriftClient.stub!(:client).and_return(@client)

      # Blur Query model
      @blur_query = FactoryGirl.create :blur_query
      @zookeeper.stub!(:blur_queries).and_return(@blur_query)
    end

    describe "GET index" do
      context "when an HTML request" do
        it "should render the index template" do
          get :index
          response.should render_template :index
        end
      end
    end

    describe "GET refresh" do
      before do
        @blur_queries = FactoryGirl.create_list :blur_query, 3
        @blur_queries.each do |query|
          query.stub!(:summary).and_return Hash.new
        end
        @zookeeper.stub!(:refresh_queries).and_return @blur_queries
      end

      it "it retrieves the refresh_queries" do
        @zookeeper.should_receive(:refresh_queries).with(kind_of(ActiveSupport::TimeWithZone))
        get :refresh, :time_length => 1, :format => :json
      end

      it "it gets the summary on each of the queries" do
        @blur_queries.each do |query|
          query.should_receive(:summary).with(@user)
        end
        get :refresh, :time_length => 1, :format => :json
      end

      it "it should set the root to aadata for the data table lib" do
        get :refresh, :time_length => 1, :format => :json
        response.body.should include("aaData")
      end
    end

    describe "GET show" do
      it "should render the more_info partial when the request is html" do
        get :show, :id => @blur_query.id, :format => :html
        response.should render_template(:partial => '_more_info')
      end
    end

    describe "PUT cancel" do
      before do
        BlurQuery.stub!(:find).and_return(@blur_query)
        @blur_query.stub!(:cancel)
      end

      it "should assign @blur_query to be the blur query specified by the id parameter" do
        BlurQuery.should_receive(:find).with('1')
        put :update, :id => @blur_query.id
        assigns(:blur_query).should == @blur_query
      end

      it "should not cancel a running query if cancel param is false" do
        @blur_query.should_not_receive(:cancel)
        put :update, :cancel => 'false', :id => '1'
      end

      it "should cancel a running query if cancel param is true" do
        @blur_query.should_receive(:cancel)
        put :update, :cancel => 'true', :id => '1'
      end

      it "should render the blur_query partial" do
        put :update, :cancel => 'false', :id => '1'
        response.should render_template(:partial => '_blur_query')
      end

      it "should log an audit event when a query is canceled" do
        Audit.should_receive :log_event
        put :update, :cancel => 'true', :id => '1'
      end
    end
  end
end
