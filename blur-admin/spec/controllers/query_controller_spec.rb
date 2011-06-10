require "spec_helper"

class CLIENT
  def tablelist
    ["table1", "table2", "table3"]
  end
end

describe QueryController do
  setup do
    @client = CLIENT
  end

  describe "show" do
    it "gets the table list and columns" do
      get :show
      response.should render_template "show"
    end
  end
  
  describe "filters" do
    it "gets the columns and columns families for the filter" do
      #TODO: mock
      #get :filters, :table => "employee_super_mart"
      #response.should render_template "filters"
    end
  end

  #before do
    #@bq = mock_model(BlurQuery)
    #BlurQuery.should_receive(:new).and_return(@bq)
  #end

  describe "create" do
    it "works when column_family & record_count < count & families_include"
    it "works when column_family & record_count < count & !families_include"
    it "works when column_family & !record_count < count & families_include"
    it "works when column_family & !record_count < count & !families_include"
    it "works when column_family & families_include"
    it "works when column_family & !families_include"
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
