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
    it "1 works when column_family & record_count < count & families_include" do
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
      @client.should_receive(:schema).with('table').and_return(test_schema)

      get :create, :t => "table", :q => "query", :column_data => ["family_table1", "column_table1_deptNo", "column_table1_moreThanOneDepartment", "column_table1_name"]
      response.should render_template "create"
    end
    
    it "2 works when column_family & record_count < count & !families_include" do
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

      get :create, :t => "table", :q => "query", :column_data => ["column_table1_deptNo", "column_table1_moreThanOneDepartment", "column_table1_name"]
      response.should render_template "create"
    end

    it "3 works when column_family & !record_count < count & families_include" do
      set = Set.new ['deptNo', 'moreThanOneDepartment', 'name']
      test_schema = Blur::Schema.new :columnFamilies => {'table1'=> set, 'table2' => set}

      test_col1 = Blur::Column.new :name => 'deptNo', :values => ['val1', 'val2', 'val3']
      test_col2 = Blur::Column.new :name => 'moreThanOneDepartment', :values => []
      test_col3 = Blur::Column.new :name => 'name', :values => ['val1']

      test_cf1 = Blur::ColumnFamily.new :records => {'key1' => [test_col1, test_col2, test_col3]}, :family => 'table1'

      test_cf2 = Blur::ColumnFamily.new :records => {'key1' => [test_col1], 'key2' => [test_col1, test_col2, test_col3]}, :family => 'table2'
      set2 = Set.new [test_cf2, test_cf1]
      test_row2 = Blur::Row.new :id => 'string' , :columnFamilies => set2
      test_rowresult2 = Blur::FetchRowResult.new :row => test_row2
      test_fetchresult2 = Blur::FetchResult.new :rowResult => test_rowresult2
      test_result2 = Blur::BlurResult.new :fetchResult => test_fetchresult2

      test_query = Blur::BlurResults.new :results => [test_result2], :totalResults => 1 #test_result1,

      @client.should_receive(:query).and_return(test_query)
      @client.should_receive(:schema).with('table').and_return(test_schema)
      @client.should_receive(:schema).with('table').and_return(test_schema)

      get :create, :t => "table", :q => "query", :column_data => ["family_table1", "column_table1_deptNo", "column_table1_moreThanOneDepartment", "column_table1_name", "family_table2", "column_table2_deptNo", "column_table2_moreThanOneDepartment", "column_table2_name"]
      response.should render_template "create"
    end

    it "4 works when column_family & !record_count < count & !families_include" do
      test_col1 = Blur::Column.new :name => 'deptNo', :values => ['val1', 'val2', 'val3']
      test_col2 = Blur::Column.new :name => 'moreThanOneDepartment', :values => ['val1', 'val2', 'val3']
      test_col3 = Blur::Column.new :name => 'name', :values => ['val1', 'val2', 'val3']

      test_cf1 = Blur::ColumnFamily.new :records => {'key1' => [test_col1, test_col2, test_col3]}, :family => 'table1'

      test_cf2 = Blur::ColumnFamily.new :records => {'key1' => [test_col1], 'key2' => [test_col1, test_col2, test_col3]}, :family => 'table2'
      set2 = Set.new [test_cf2, test_cf1]
      test_row2 = Blur::Row.new :id => 'string' , :columnFamilies => set2
      test_rowresult2 = Blur::FetchRowResult.new :row => test_row2
      test_fetchresult2 = Blur::FetchResult.new :rowResult => test_rowresult2
      test_result2 = Blur::BlurResult.new :fetchResult => test_fetchresult2

      test_query = Blur::BlurResults.new :results => [test_result2], :totalResults => 1

      @client.should_receive(:query).and_return(test_query)

      get :create, :t => "table", :q => "query", :column_data => ["column_table1_deptNo", "column_table1_moreThanOneDepartment", "column_table1_name", "column_table2_deptNo", "column_table2_moreThanOneDepartment", "column_table2_name"]
      response.should render_template "create"
    end

    it "5 works when !column_family & families_include" do
      set = Set.new ['deptNo', 'moreThanOneDepartment', 'name']
      test_schema = Blur::Schema.new :columnFamilies => {'table1'=> set, 'table2'=> set}

      test_col1 = Blur::Column.new :name => 'deptNo', :values => ['val1', 'val2', 'val3']
      test_col2 = Blur::Column.new :name => 'moreThanOneDepartment', :values => []
      test_col3 = Blur::Column.new :name => 'name', :values => ['val1']
      test_cf1 = Blur::ColumnFamily.new :records => {'key1' => [test_col1, test_col2, test_col3]}, :family => 'table1'

      set1 = Set.new [test_cf1]
      test_row1 = Blur::Row.new :id => 'string' , :columnFamilies => set1
      test_rowresult1 = Blur::FetchRowResult.new :row => test_row1
      test_fetchresult1 = Blur::FetchResult.new :rowResult => test_rowresult1
      test_result1 = Blur::BlurResult.new :fetchResult => test_fetchresult1

      test_cf2 = Blur::ColumnFamily.new :records => {'key1' => [test_col1], 'key2' => [test_col1, test_col2, test_col3]}, :family => 'table2'
      set2 = Set.new [test_cf2]
      test_row2 = Blur::Row.new :id => 'string' , :columnFamilies => set2
      test_rowresult2 = Blur::FetchRowResult.new :row => test_row2
      test_fetchresult2 = Blur::FetchResult.new :rowResult => test_rowresult2
      test_result2 = Blur::BlurResult.new :fetchResult => test_fetchresult2

      test_query = Blur::BlurResults.new :results => [test_result1, test_result2], :totalResults => 2

      @client.should_receive(:query).and_return(test_query)
      @client.should_receive(:schema).with('table').and_return(test_schema)

      get :create, :t => "table", :q => "query", :column_data => ["family_table1", "column_table1_deptNo", "column_table1_moreThanOneDepartment", "column_table1_name"]
      response.should render_template "create"
    end

    it "6 works when !column_family & !families_include" do
      test_col1 = Blur::Column.new :name => 'deptNo', :values => ['val1', 'val2', 'val3']
      test_col2 = Blur::Column.new :name => 'moreThanOneDepartment', :values => []
      test_col3 = Blur::Column.new :name => 'name', :values => ['val1']
      test_cf1 = Blur::ColumnFamily.new :records => {'key1' => [test_col1, test_col2, test_col3]}, :family => 'table1'

      set1 = Set.new [test_cf1]
      test_row1 = Blur::Row.new :id => 'string' , :columnFamilies => set1
      test_rowresult1 = Blur::FetchRowResult.new :row => test_row1
      test_fetchresult1 = Blur::FetchResult.new :rowResult => test_rowresult1
      test_result1 = Blur::BlurResult.new :fetchResult => test_fetchresult1

      test_cf2 = Blur::ColumnFamily.new :records => {'key1' => [test_col1], 'key2' => [test_col1, test_col2, test_col3]}, :family => 'table2'
      set2 = Set.new [test_cf2]
      test_row2 = Blur::Row.new :id => 'string' , :columnFamilies => set2
      test_rowresult2 = Blur::FetchRowResult.new :row => test_row2
      test_fetchresult2 = Blur::FetchResult.new :rowResult => test_rowresult2
      test_result2 = Blur::BlurResult.new :fetchResult => test_fetchresult2

      test_query = Blur::BlurResults.new :results => [test_result1, test_result2], :totalResults => 2

      @client.should_receive(:query).and_return(test_query)

      get :create, :t => "table", :q => "query", :column_data => ["column_table1_deptNo", "column_table1_moreThanOneDepartment", "column_table1_name"]
      response.should render_template "create"
    end

    it "works when superQueryOn is false" do
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
      @client.should_receive(:schema).with('table').and_return(test_schema)

      get :create, :t => "table", :q => "query", :column_data => ["family_table1", "column_table1_deptNo", "column_table1_moreThanOneDepartment", "column_table1_name"], :s => false
      response.should render_template "create"
    end
  end
end
