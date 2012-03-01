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
        @zookeeper  = Factory.stub :zookeeper
        Zookeeper.stub(:find_by_id).and_return(@zookeeper)
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
        mock_time = Time.local(2011, 6, 28, 10, 20, 30)
        Time.stub(:now).and_return(mock_time + 30.seconds)

        # Set up association chain
        @zookeeper  = FactoryGirl.create :zookeeper
        @blur_table = FactoryGirl.create :blur_table
        @blur_query = FactoryGirl.create :blur_query, :created_at => mock_time

        @zookeeper.stub(:blur_tables).and_return([@blur_table])
        BlurQuery.stub_chain(:joins, :where, :includes, :order, :filter_on_time_range).and_return([@blur_query])
        @blur_query.stub(:zookeeper).and_return(@zookeeper)

        # ApplicationController.current_zookeeper
        Zookeeper.stub(:find_by_id).and_return(@zookeeper)
        Zookeeper.stub_chain(:order, :first).and_return @zookeeper

      end
      it "assigns the current zookeeper to @current_zookeeper" do
        pending "New Table Implementation"
        get :refresh, :time_since_refresh => ''
        assigns(:current_zookeeper).should == @zookeeper
      end
    end

    describe "PUT update" do
      before do
        BlurQuery.stub!(:find).and_return(@blur_query)
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
      it "should render the more_info partial" do
        BlurQuery.stub_chain(:includes, :find)
        get :more_info, :id => '1'
        response.should render_template(:partial => '_more_info')
      end

      it "should assign @blur_query to be the blur query specified by the id parameter" do
        BlurQuery.stub_chain(:includes, :find).and_return(@blur_query)
        get :more_info, :id => '1'
        assigns(:blur_query).should == @blur_query
      end
    end
  end
end
