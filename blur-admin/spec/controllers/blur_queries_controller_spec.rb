require "spec_helper"

describe BlurQueriesController do
  describe "actions" do
    before do
      @client = mock(Blur::Blur::Client)
      BlurThriftClient.stub!(:client).and_return(@client)
      
      @user = User.new
      @ability = Ability.new @user
      @ability.stub!(:can?).and_return(true)
      controller.stub!(:current_ability).and_return(@ability)

      @blur_query = FactoryGirl.create :blur_query
      @user = User.new
      controller.stub!(:current_user).and_return(@user)
    end

    describe "GET index" do
      before do
        @zookeeper  = FactoryGirl.create :zookeeper
        Zookeeper.stub(:find_by_id).and_return(@zookeeper)
        Zookeeper.stub!(:first).and_return(@zookeeper)
        Zookeeper.stub(:order).and_return [@zookeeper]
      end

      it "assigns the current zookeeper to @current_zookeeper" do
        get :index
        assigns(:current_zookeeper).should == @zookeeper
      end

      context "when an HTML request" do
        it "should render the index template" do
          get :index
          response.should render_template "index"
        end
      end
    end

    describe "GET refresh" do
      before do
        @zookeeper  = FactoryGirl.create :zookeeper_with_blur_queries

        # ApplicationController.current_zookeeper
        Zookeeper.stub(:find_by_id).and_return(@zookeeper)
        Zookeeper.stub!(:first).and_return(@zookeeper)
        Zookeeper.stub_chain(:order, :first).and_return @zookeeper
      end

      it "assigns the current zookeeper to @current_zookeeper" do
        get :refresh, :time_length => 1
        assigns(:current_zookeeper).should == @zookeeper
      end

      it "calls the sql with the proper parameters" do
        @zookeeper.blur_queries.should_receive(:where)
          .with("blur_queries.updated_at > ? and blur_tables.status = ?", kind_of(ActiveSupport::TimeWithZone), 4)
          .and_return([])
        get :refresh, :time_length => 1
      end

      it "calls summary on each of the queries" do
        query = @zookeeper.blur_queries.first
        query.should_receive(:summary).with(@user).and_return({})
        @zookeeper.blur_queries.should_receive(:where)
          .with("blur_queries.updated_at > ? and blur_tables.status = ?", kind_of(ActiveSupport::TimeWithZone), 4)
          .and_return([query])
        get :refresh, :time_length => 1
      end
    end

    describe "PUT update" do
      before do
        BlurQuery.stub!(:find).and_return(@blur_query)
        @blur_query.stub!(:cancel)
      end

      it "should assign @blur_query to be the blur query specified by the id parameter" do
        BlurQuery.should_receive(:find).with('1')
        put :update, :id => '1'
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
    end

    describe "GET more_info" do
      it "should assign @blur_query to be the blur query specified by the id parameter" do
        BlurQuery.stub_chain(:find).and_return(@blur_query)
        get :more_info, :id => '1'
        assigns(:blur_query).should == @blur_query
      end

      it "should render the more_info partial" do
        BlurQuery.stub_chain(:find)
        get :more_info, :id => '1'
        response.should render_template(:partial => '_more_info')
      end
    end
  end
end
