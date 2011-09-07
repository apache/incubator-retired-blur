require "spec_helper"

describe SearchController do

  before (:each) do
    @ability = Ability.new User.new
    @user = User.new
    @ability.stub(:can?).and_return(true)
    controller.stub(:current_ability).and_return(@ability)
    controller.stub(:current_user).and_return(@user)
  end

  describe "show" do
    before :each do
      @blur_tables = [Factory.stub(:blur_table), Factory.stub(:blur_table)]
      @blur_table = @blur_tables[0]
      # Set up association chain
      @zookeeper  = Factory.stub :zookeeper

      # ApplicationController.current_zookeeper
      Zookeeper.stub(:find_by_id).and_return(@zookeeper)
      # Zookeeper.stub_chain(:order, :first).and_return @zookeeper
      # ApplicationController.zookeepers
      Zookeeper.stub(:order).and_return [@zookeeper]

      @search = Factory.stub :search
      @user.stub_chain(:searches, :order).and_return [@search]
    end

    it "renders the show template" do
      get :show
      response.should render_template 'show'
    end
    
    it "find and assign tables, and columns" do
      @zookeeper.stub_chain(:blur_tables, :order, :all).and_return(@blur_tables)
      get :show
      assigns(:blur_tables).should == @blur_tables
      assigns(:blur_table).should == @blur_table
      assigns(:columns).should == @blur_table.schema
    end

    describe "when no tables are available" do
      it "find and assign tables and columns" do
        @zookeeper.stub_chain(:blur_tables, :order, :all).and_return []
        get :show
        assigns(:blur_tables).should == []
        assigns(:blur_table).should be nil
        assigns(:columns).should be nil
      end
    end
  end
  
  describe "filters" do
    before :each do 
      @blur_table = Factory.stub( :blur_table )
      BlurTable.stub(:find).and_return @blur_table
    end

    it "renders the filters template" do
      get :filters, :blur_table_id => @blur_table.id
      response.should render_template "filters"
    end

    it "should find the new columns" do
      BlurTable.should_receive(:find).with(@blur_table.id)
      get :filters, :blur_table_id => @blur_table.id
    end
    
    it "should return an empty array to columns when no blur table is selected" do
      BlurTable.should_receive(:find).and_return(nil)
      get :filters, :blur_table_id => @blur_table.id
      response.should render_template "filters"
    end
  end

  describe "GET create" do
    before :each do
      @search     = Factory.stub :search
      @blur_table = Factory.stub :blur_table
      @user       = Factory.stub :user
      @preference = Factory.stub :preference
      @client = mock(Blur::Blur::Client)
      Preference.stub(:find_or_create_by_user_id_and_pref_type).and_return(@preference)
      BlurTable.stub(:find).and_return(@blur_table)
      Search.stub(:new).and_return(@search)
      Search.stub(:find).and_return(@search)
      User.stub(:find).and_return(@user)
      controller.stub(:current_user).and_return(@user)
      results = mock 'blur_results', :totalResults => @search.fetch,
                                     :realTime => 10,
                                     :results => [create_blur_result(@search)]
      @search.stub(:fetch_results).and_return(results)

      # Set up association chain
      @zookeeper  = Factory.stub :zookeeper

      # ApplicationController.current_zookeeper
      Zookeeper.stub(:find_by_id).and_return(@zookeeper)
      # Zookeeper.stub_chain(:order, :first).and_return @zookeeper
    end

    def create_blur_result(search)
      ###### Hierarchy of Blur::BlurResults 8/10 #######
      #blur_results     = mock 'results'          # Blur::BlurResults
      #results          = []                      # Array
      #result           = mock 'result'           # Blur::BlurResult
      #fetch_result     = mock 'fetch_result'     # Blur::FetchResult
      #fetch_row_result = mock 'fetch_row_result' # Blur::FetchRowResult
      #row              = mock 'row'              # Blur::Row
      #records          = []                      # Array
      #record           = mock 'record'           # Blur::Record
      #columns          = []                      # Array
      #column           = mock 'column'           # Blur::Column
      #

      schema = search.columns
      column_families = []
      schema.each_key do |column_family_name|
        columns = []
        schema[column_family_name].each do |column|
          column = mock 'column', :name => column, :value => "value_1"
          columns << column
        end
        column_family =  mock 'record', :recordId => rand(10000), :columns => columns, :family => column_family_name 
        column_families << column_family
      end

      row = mock 'row', :records => column_families, :id => rand(10000)
      fetch_row_result = mock 'fetch_row_result', :row => row
      fetch_result     = mock 'fetch_result', :rowResult => fetch_row_result
      blur_result      = mock 'result', :fetchResult => fetch_result
    end

    describe "when creating a new search" do
      it "renders the create partial" do

        get :create, :super_query  => @search.super_query,
                     :result_count => @search.fetch,
                     :offset       => @search.offset,
                     :query_string => @search.query,
                     :column_data  => ["neighborhood", @search.raw_columns].flatten

        response.should render_template "create"
      end
    end
    describe "when running an existing search" do
      it "fetches the saved search object" do
        Search.should_receive(:find).with(@search.id)
        get :create, :search_id  => @search.id
      end
    end
    it "assigns the @schema variable to hold the sorted column families and columns of the search" do
      get :create, :search_id  => @search.id
      assigns(:schema).keys.should == %w[ColumnFamily2 ColumnFamily1 ColumnFamily3]
    end
    it "assigns the @result_count and @result_time instance variables" do
      get :create, :search_id  => @search.id
      assigns(:result_count).should == @search.fetch
      assigns(:result_time).should == 10
    end
    it "correctly parses a result from blur" do
      pending "Is there a better way to do this?"
      get :create, :search_id  => @search.id
      assigns(:results).should == ""
    end
    it "correctly sets the schema variable" do
      pending "Is there a better way to do this?"
      get :create, :search_id  => @search.id
      assigns(:schema).should == "" 
    end
  end

  describe "load" do
    before(:each) do
      @search = Factory.stub :search
      Search.stub(:new).and_return(@search)
    end

    it "renders the proper json for a search" do
      Search.stub(:find).and_return(@search)
      get :load, :search_id => 1
      @return = {:saved => @search, :success => true}
      search = JSON.parse @search.to_json
      search["search"]["columns"] = @search.raw_columns
      response.body.should == {:saved => search, :success => true}.to_json
    end
  end

  describe "delete" do
    before(:each) do
      @search = Factory.stub :search
      Search.stub(:new).and_return(@search)
      Search.stub(:find).and_return(@search)
      Search.stub(:delete)
      BlurTable.stub(:find)
    end

    it "renders the saved partial" do
      delete :delete, :search_id => 1, :blur_table => 1
      response.should render_template 'saved'
    end

    it "finds the correct table and deletes it from the DB" do
      Search.should_receive(:find).with(1)
      @search.should_receive(:delete)
      delete :delete, :search_id => 1, :blur_table => 1
    end
  end
  
  describe "reload" do
    before(:each) do
      @search = Factory.stub :search
      @user.stub(:searches).and_return [@search]
      Search.stub(:new).and_return(@search)
      Search.stub(:find).and_return(@search)
      BlurTable.stub(:find)
    end

    it "renders the saved partial" do
      get :reload, :blur_table => 1
      response.should render_template 'saved'
    end
  end
  
  describe "save" do
    before(:each) do
    end

    it "saves and renders the saved partial" do
      BlurTable.stub(:find)
      @search = Factory.stub :search
      @user.stub(:searches).and_return [@search]
      @user.stub(:id).and_return [1]      
      Search.stub(:find).and_return(@search)
      Search.stub(:create).and_return @search
      Search.should_receive(:create)
      get :save, :column_data => ["family_table1", "column_table1_deptNo", "column_table1_moreThanOneDepartment", "column_table1_name"]
      response.should render_template 'saved'
    end
  end
end
