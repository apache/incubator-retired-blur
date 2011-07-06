require "spec_helper"

describe SearchController do

  before (:each) do
    @client = mock(Blur::Blur::Client)
    BlurThriftClient.stub!(:client).and_return(@client)
    
    set = ['deptNo', 'moreThanOneDepartment', 'name']
    @schema1 = {:columnFamilies => {'table1'=> set}}
    @schema2 = {:columnFamilies => {'table1'=> set, 'table2' => set}}
    @ability = Ability.new User.new
    @ability.stub!(:can?).and_return(true)
    controller.stub!(:current_ability).and_return(@ability)
  end

  describe "show" do
    before :each do
      @table = Factory.stub( :blur_table )
      BlurTable.stub(:all).and_return [@table]
      @current_user.stub(:searches).and_return [mock(Search)]
    end

    it "renders the show template" do
      get :show
      response.should render_template "show"
    end
    
    it "find and assign tables, and columns" do
      BlurTable.should_receive(:all)
      get :show
      assigns(:blur_tables).should == [@table]
      assigns(:columns).should == @table.schema["columnFamilies"]
    end

    it "find and assign tables and columns when no tables are available" do
      BlurTable.should_receive(:all).and_return []
      get :show
      assigns(:blur_tables).should == []
      assigns(:columns).should be nil
    end
  end
  
  describe "filters" do
    before :each do
      @table = Factory.stub( :blur_table )
      BlurTable.stub(:find).and_return @table
    end

    it "renders the filters template" do
      get :filters, :blur_table_id => @table.id
      response.should render_template "filters"
    end

    it "should assign columns" do
      BlurTable.should_receive(:find).with(@table.id)
      get :filters, :blur_table_id => @table.id
      assigns(:columns).should == @table.schema["columnFamilies"]
    end
  end

  describe "create" do
    before (:each) do
      test1_col1 = Blur::Column.new :name => 'deptNo', :values => ['val1', 'val2', 'val3']
      test1_col2 = Blur::Column.new :name => 'moreThanOneDepartment', :values => ['val1', 'val2', 'val3']
      test1_col3 = Blur::Column.new :name => 'name', :values => ['val1', 'val2', 'val3']
      @test1_cf1 = Blur::ColumnFamily.new :records => {'key1' => [test1_col1, test1_col2, test1_col3]}, :family => 'table1'
      @test1_cf2 = Blur::ColumnFamily.new :records => {'key1' => [test1_col1], 'key2' => [test1_col1, test1_col2, test1_col3]}, :family => 'table2'

      test1_set1 = Set.new [@test1_cf1]
      test1_result1 = create_blur_result(:set => test1_set1)
      @test1_query = Blur::BlurResults.new :results => [test1_result1], :totalResults => 1

      test2_col1 = Blur::Column.new :name => 'deptNo', :values => ['val1', 'val2', 'val3']
      test2_col2 = Blur::Column.new :name => 'moreThanOneDepartment', :values => []
      test2_col3 = Blur::Column.new :name => 'name', :values => ['val1']
      @test2_cf1 = Blur::ColumnFamily.new :records => {'key1' => [test2_col1, test2_col2, test2_col3]}, :family => 'table1'
      @test2_cf2 = Blur::ColumnFamily.new :records => {'key1' => [test2_col1], 'key2' => [test2_col1, test2_col2, test2_col3]}, :family => 'table2'

      test2_set1 = Set.new [@test2_cf1]
      test2_set2 = Set.new [@test2_cf2]
      test2_result1 = create_blur_result(:set => test2_set1)
      test2_result2 = create_blur_result(:set => test2_set2)
      @test2_query = Blur::BlurResults.new :results => [test2_result1, test2_result2], :totalResults => 2

      @table = Factory.stub :blur_table
      BlurTable.stub(:find).with(@table.id).and_return(@table)

      @search = Search.new(:super_query => true, :columns => JSON.generate(["neighborhood", "family_titleHistory", "column_titleHistory_fromDate", "column_titleHistory_title"]), fetch: 25, offset: 5, name: "default", query: "employee.name:bob", blur_table_id: 17, user_id: 1)
      Search.stub(:new).and_return(@search)
    end

    def create_blur_result(options)
      row = Blur::Row.new :id => 'string' , :columnFamilies => options[:set]
      rowresult = Blur::FetchRowResult.new :row => row
      fetchresult = Blur::FetchResult.new :rowResult => rowresult
      result = Blur::BlurResult.new :fetchResult => fetchresult
      result
    end
    
    it "renders the create template when column_family & record_count < count & families_include" do
      pending "Actually understing the action and these tests..."
      @client.should_receive(:query).and_return(@test1_query)
      get :create
      response.should render_template "create"
    end
    
    it "renders the create template when column_family & record_count < count & !families_include" do
      pending "Actually understing the action and these tests..."
      @client.should_receive(:query).and_return(@test1_query)
      get :create
      response.should render_template "create"
    end

    it "renders the create template when column_family & !record_count < count & families_include" do
      pending "Actually understing the action and these tests..."
      set2 = Set.new [@test2_cf2, @test2_cf1]
      test_result2 = create_blur_result(:set => set2)
      test_query = Blur::BlurResults.new :results => [test_result2], :totalResults => 1

      @client.should_receive(:query).and_return(test_query)
      @client.should_receive(:schema).with('table').and_return(@test_schema2)
      @client.should_receive(:schema).with('table').and_return(@test_schema2)
      get :create, :blur_table => @table.id, :query_string => "query", :result_count => 25, :column_data => ["family_table1", "column_table1_deptNo", "column_table1_moreThanOneDepartment", "column_table1_name", "family_table2", "column_table2_deptNo", "column_table2_moreThanOneDepartment", "column_table2_name"]
      response.should render_template "create"
    end

    it "renders the create template when column_family & !record_count < count & !families_include" do
      pending "Actually understing the action and these tests..."
      set2 = Set.new [@test1_cf2, @test1_cf1]
      test_result2 = create_blur_result(:set => set2)
      test_query = Blur::BlurResults.new :results => [test_result2], :totalResults => 1

      @client.should_receive(:query).and_return(test_query)
      get :create, :blur_table => @table.id, :query_string => "query", :result_count => 25, :column_data => ["column_table1_deptNo", "column_table1_moreThanOneDepartment", "column_table1_name", "column_table2_deptNo", "column_table2_moreThanOneDepartment", "column_table2_name"]
      response.should render_template "create"
    end

    it "renders the create template when !column_family & families_include" do
      pending "Actually understing the action and these tests..."
      @client.should_receive(:query).and_return(@test2_query)
      get :create, :blur_table => @table.id, :query_string => "query", :result_count => 25, :column_data => ["family_table1", "column_table1_deptNo", "column_table1_moreThanOneDepartment", "column_table1_name"]
      response.should render_template "create"
    end

    it "renders the create template when !column_family & !families_include" do
      pending "Actually understing the action and these tests..."
      @client.should_receive(:query).and_return(@test2_query)
      get :create, :blur_table => @table.id, :query_string => "query", :result_count => 25, :column_data => ["column_table1_deptNo", "column_table1_moreThanOneDepartment", "column_table1_name"]
      response.should render_template "create"
    end

    it "renders the create template when superQueryOn is false" do
      pending "Actually understing the action and these tests..."
      @client.should_receive(:query).and_return(@test1_query)
      get :create, :blur_table => @table.id, :query_string => "query", :result_count => 25, :column_data => ["family_table1", "column_table1_deptNo", "column_table1_moreThanOneDepartment", "column_table1_name"], :super_query => false
      response.should render_template "create"
    end
  end
end
