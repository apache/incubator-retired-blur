require "spec_helper"

describe BlurQueriesController do
  before do
    @client = mock(Blur::Blur::Client)
    BlurThriftClient.stub!(:client).and_return(@client)

    @ability = Ability.new User.new
    @ability.stub!(:can?).and_return(true)
    controller.stub!(:current_ability).and_return(@ability)

    @blur_query = Factory.stub :blur_query

    @ability = Ability.new User.new
    @user = User.new
    @ability.stub!(:can?).and_return(true)
    controller.stub!(:current_ability).and_return(@ability)
    controller.stub!(:current_user).and_return(@user)
    
    @user.stub(:saved_filters).and_return (Factory.stub :filter_preference).value
  end

  describe "GET index" do
    before do
      mock_time = Time.local(2011, 6, 28, 10, 20, 30)
      Time.stub(:now).and_return(mock_time + 30.seconds)

      # Set up association chain
      @zookeeper  = Factory.stub :zookeeper
      @blur_table = Factory.stub :blur_table
      @blur_query = Factory.stub :blur_query, :created_at => mock_time

      @zookeeper.stub_chain(:blur_tables, :where).and_return([@blur_table])
      BlurQuery.stub_chain(:joins, :where, :includes, :order, :filter_on_time_range).and_return([@blur_query])

      # ApplicationController.current_zookeeper
      Zookeeper.stub(:find_by_id).and_return(@zookeeper)
      # Zookeeper.stub_chain(:order, :first).and_return @zookeeper
      # ApplicationController.zookeepers
      Zookeeper.stub(:order).and_return [@zookeeper]

    end
    # it "assigns the collection all zookeepers to @zookeepers" do
    #       get :index
    #       assigns(:zookeepers).should == [@zookeeper]
    #     end

    it "assigns the current zookeeper to @current_zookeeper" do
      get :index
      assigns(:current_zookeeper).should == @zookeeper
    end

    it "should assign @blur_tables to be the collection of all blur tables" do
      @zookeeper.should_receive(:blur_tables)
      get :index
      assigns(:blur_tables).should == [@blur_table]
    end

    it "should assign @blur_queries to be the collection of blur queries" do
      get :index
      assigns(:blur_queries).should == [@blur_query]
    end

    it "filters blur queries to running queries within the past minute" do
      pending "Test active relations"
      BlurQuery.should_receive(:where).with(:created_at => Time.now - 1.minutes..Time.now, :running => :true)
      get :index
    end

    it "filters blur queries by zookeeper" do
      other_query = Factory.stub :blur_query
      other_query.stub(:zookeeper).and_return(Factory.stub :zookeeper)
      BlurQuery.stub(:all).and_return [@blur_query, other_query]
      get :index
      assigns(:blur_queries).should_not include other_query
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
      @zookeeper  = Factory.stub :zookeeper
      @blur_table = Factory.stub :blur_table
      @blur_query = Factory.stub :blur_query, :created_at => mock_time

      @zookeeper.stub(:blur_tables).and_return([@blur_table])
      BlurQuery.stub_chain(:joins, :where, :includes, :order, :filter_on_time_range).and_return([@blur_query])
      @blur_query.stub(:zookeeper).and_return(@zookeeper)

      # ApplicationController.current_zookeeper
      Zookeeper.stub(:find_by_id).and_return(@zookeeper)
      Zookeeper.stub_chain(:order, :first).and_return @zookeeper

    end
    it "assigns the current zookeeper to @current_zookeeper" do
      get :refresh, :time_since_refresh => ''
      assigns(:current_zookeeper).should == @zookeeper
    end

    it "should assign @blur_queries to be the collection of blur queries" do
      get :refresh, :time_since_refresh => ''
      assigns(:blur_queries).should == [@blur_query]
    end

    it "filters blur queries to within the past minute if no time params given" do
      pending "Test active relations"
      BlurQuery.should_receive(:all).with(:conditions => {:created_at => Time.now - 1.minutes..Time.now},
                                          :order=>"created_at desc")
      get :refresh, :time_since_refresh => ''
    end

    it "filters blur queries to within a specified time if given a time parameter" do
      pending "Test active relations"
      BlurQuery.should_receive(:all).with :conditions => {:created_at => Time.now - 60.minutes..Time.now },
                                          :order=>"created_at desc"
      get :refresh, :created_at_time => '60', :time_since_refresh => ''
    end

    it "filters blur queries to within a specified updated_at if given the parameter" do
      pending "Test active relations"
      BlurQuery.should_receive(:all).with(:conditions => {:created_at => Time.now - 1.minutes..Time.now,
                                                          :updated_at => Time.now - 14.seconds..Time.now},
                                          :order=>"created_at desc")
      get :refresh, :time_since_refresh => '14'

    end

    it "filters blur queries by super query status if given a super_query_on parameter" do
      pending "Test active relations"
      BlurQuery.should_receive(:all).with(:conditions => {:super_query_on => true,
                                                          :created_at => Time.now - 1.minutes..Time.now},
                                          :order=>"created_at desc")
      get :refresh, :super_query_on => 'true', :time_since_refresh => ''
    end

    it "filters blur queries by running status if given a running parameter" do
      pending "Test active relations"
      BlurQuery.should_receive(:all).with(:conditions => {:running => true,
                                                          :created_at => Time.now - 1.minutes..Time.now},
                                          :order=>"created_at desc")
      get :refresh, :running => 'true', :time_since_refresh => ''
    end

    it "filters blur queries by interrupted status if given an interrupted parameter" do
      pending "Test active relations"
      BlurQuery.should_receive(:all).with(:conditions => {:interrupted => true,
                                                          :created_at => Time.now - 1.minutes..Time.now},
                                          :order=>"created_at desc")
      get :refresh, :interrupted => 'true', :time_since_refresh => ''
    end

    it "filters blur queries by table if given a blur_table_id parameter" do
      pending "Test active relations"
      BlurQuery.should_receive(:all).with(:conditions => {:blur_table_id => '1',
                                                          :created_at => Time.now - 1.minutes..Time.now},
                                          :order=>"created_at desc")
      get :refresh, :blur_table_id => '1', :time_since_refresh => ''
    end

    it "filters blur queries by zookeeper" do
      pending "Test active relations"
      other_query = Factory.stub :blur_query
      other_query.stub(:zookeeper).and_return(Factory.stub :zookeeper)
      BlurQuery.stub(:all).and_return [@blur_query, other_query]
      get :refresh, :time_since_refresh => ''
      assigns(:blur_queries).should_not include other_query
    end

    it "should render the _blur_query collection" do
      get :refresh, :time_since_refresh => ''
      response.should render_template '_blur_query', :collection => [@blur_query]
    end

    context "when an XHR (ajax) request" do
      it "should render the _blur_query collection" do
        xhr :get, :refresh, :time_since_refresh => ''
        response.should render_template '_blur_query', :collection => [@blur_query]
      end
    end
  end

  describe "PUT update" do
    before do
      BlurQuery.stub(:find).and_return(@blur_query)
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
  end

  describe "GET more_info" do
    it "should render the more_info partial" do
      BlurQuery.stub_chain(:includes, :find)
      get :more_info, :id => '1'
      response.should render_template(:partial => '_more_info')
    end

    it "should assign @blur_query to be the blur query specified by the id parameter" do
      pending "Test active relations"
      BlurQuery.stub_chain(:find, :includes)
      BlurQuery.should_receive(:find).with('1').and_return(@blur_query)
      get :more_info, :id => '1'
      assigns(:blur_query).should == @blur_query
    end
  end
end
