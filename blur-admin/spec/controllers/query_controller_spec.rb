require "spec_helper"

describe QueryController do

  before (:each) do
    @client = mock(Blur::Blur::Client)
    controller.stub!(:thrift_client).and_return(@client)
    controller.stub!(:close_thrift)
  end

  describe "show" do
    it "gets the table list and columns" do
      @client.should_receive(:tableList).and_return(['blah'])
      @client.should_receive(:schema).with('blah').and_return(Blur::Schema.new)
      get :show
      response.should render_template "show"
    end
  end
  
  describe "filters" do
    it "gets the columns and columns families for the filter" do
      @client.should_receive(:schema).with('table1').and_return(Blur::Schema.new)
      get :filters, :table => 'table1'
      #TODO: check render
    end
  end

  describe "create" do
    it "works when column_family & record_count < count & families_include" do
      set = Set.new ['deptNo', 'moreThanOneDepartment', 'name'] 
      test_schema = Blur::Schema.new :columnFamilies => {'table1'=> set}

      test_col1 = Blur::Column.new :name => 'deptNo', :values => ['val1', 'val2', 'val3']
      test_col2 = Blur::Column.new :name => 'moreThanOneDepartment', :values => ['val1', 'val2', 'val3']
      test_col3 = Blur::Column.new :name => 'name', :values => ['val1', 'val2', 'val3']
      test_cf1 = Blur::ColumnFamily.new :records => {'key1' => [test_col1, test_col2, test_col3]}, :family => 'table1'
      set1 = Set.new [test_cf1]
      test_row1 = Blur::Row.new :id => 'string' , :columnFamilies => set1
      test_rowresult1 = Blur::FetchRowResult.new :row => test_row1
      test_fetchresult1 = Blur::FetchResult.new :rowResult => test_rowresult1
      test_result1 = Blur::BlurResult.new :fetchResult => test_fetchresult1
      test_query = Blur::BlurResults.new :results => [test_result1], :totalResults => 1

      @client.should_receive(:query).and_return(test_query)
      @client.should_receive(:schema).with('table1').and_return(test_schema)

      get :create, :t => "table1", :q => "query", :column_data => ["family_table1", "column_table1_deptNo", "column_table1_moreThanOneDepartment", "column_table1_name"]
      response.should render_template "create"
    end
    
    it "works when column_family & record_count < count & !families_include" do
      test_col1 = Blur::Column.new :name => 'deptNo', :values => ['val1', 'val2', 'val3']
      test_col2 = Blur::Column.new :name => 'moreThanOneDepartment', :values => ['val1', 'val2', 'val3']
      test_col3 = Blur::Column.new :name => 'name', :values => ['val1', 'val2', 'val3']
      test_cf1 = Blur::ColumnFamily.new :records => {'key1' => [test_col1, test_col2, test_col3]}, :family => 'table1'
      set1 = Set.new [test_cf1]
      test_row1 = Blur::Row.new :id => 'string' , :columnFamilies => set1
      test_rowresult1 = Blur::FetchRowResult.new :row => test_row1
      test_fetchresult1 = Blur::FetchResult.new :rowResult => test_rowresult1
      test_result1 = Blur::BlurResult.new :fetchResult => test_fetchresult1
      test_query = Blur::BlurResults.new :results => [test_result1], :totalResults => 1

      @client.should_receive(:query).and_return(test_query)

      get :create, :t => "table1", :q => "query", :column_data => ["column_table1_deptNo", "column_table1_moreThanOneDepartment", "column_table1_name"]
      response.should render_template "create"
    end

    it "works when column_family & !record_count < count & families_include" do
      assigns[:count] = 0

      set = Set.new ['deptNo', 'moreThanOneDepartment', 'name']
      test_schema = Blur::Schema.new :columnFamilies => {'table1'=> set}

      test_col1 = Blur::Column.new :name => 'deptNo', :values => ['val1', 'val2', 'val3']
      test_col2 = Blur::Column.new :name => 'moreThanOneDepartment', :values => ['val1', 'val2', 'val3']
      test_col3 = Blur::Column.new :name => 'name', :values => ['val1', 'val2', 'val3']
      test_cf1 = Blur::ColumnFamily.new :records => {'key1' => [test_col1, test_col2, test_col3]}, :family => 'table1'
      set1 = Set.new [test_cf1]
      test_row1 = Blur::Row.new :id => 'string' , :columnFamilies => set1
      test_rowresult1 = Blur::FetchRowResult.new :row => test_row1
      test_fetchresult1 = Blur::FetchResult.new :rowResult => test_rowresult1
      test_result1 = Blur::BlurResult.new :fetchResult => test_fetchresult1
      test_query = Blur::BlurResults.new :results => [test_result1], :totalResults => 1

      @client.should_receive(:query).and_return(test_query)
      @client.should_receive(:schema).with('table1').and_return(test_schema)

      get :create, :t => "table1", :q => "query", :column_data => ["family_table1", "column_table1_deptNo", "column_table1_moreThanOneDepartment", "column_table1_name"]
      response.should render_template "create"
    end

    it "works when column_family & !record_count < count & !families_include" do
      assigns[:count] = 0

      test_col1 = Blur::Column.new :name => 'deptNo', :values => ['val1', 'val2', 'val3']
      test_col2 = Blur::Column.new :name => 'moreThanOneDepartment', :values => ['val1', 'val2', 'val3']
      test_col3 = Blur::Column.new :name => 'name', :values => ['val1', 'val2', 'val3']
      test_cf1 = Blur::ColumnFamily.new :records => {'key1' => [test_col1, test_col2, test_col3]}, :family => 'table1'
      set1 = Set.new [test_cf1]
      test_row1 = Blur::Row.new :id => 'string' , :columnFamilies => set1
      test_rowresult1 = Blur::FetchRowResult.new :row => test_row1
      test_fetchresult1 = Blur::FetchResult.new :rowResult => test_rowresult1
      test_result1 = Blur::BlurResult.new :fetchResult => test_fetchresult1
      test_query = Blur::BlurResults.new :results => [test_result1], :totalResults => 1

      @client.should_receive(:query).and_return(test_query)

      get :create, :t => "table1", :q => "query", :column_data => ["column_table1_deptNo", "column_table1_moreThanOneDepartment", "column_table1_name"]
      response.should render_template "create"
    end

    it "works when !column_family & families_include"
    it "works when !column_family & !families_include"
    #it "displays the results for a query1" do
      #get :create, :t => "employee_super_mart", :q => "employee.name:bob", :column_data =>
        #["family_department", "column_department_deptNo", "column_department_moreThanOneDepartment", "column_department_name"]
      #response.should render_template "create"
    #end
    #it "displays the results for a query2" do
      #get :create, :t => "employee_super_mart", :q => "employee.name:bob", :column_data =>
        #["column_department_moreThanOneDepartment", "column_department_name"]
      #response.should render_template "create"
    #end
  end
end
