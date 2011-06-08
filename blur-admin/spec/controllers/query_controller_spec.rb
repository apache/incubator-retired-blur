require "spec_helper"

describe QueryController do
  describe "show" do
    it "gets the table list and columns" do
      get :show
      response.should render_template "show"
    end
  end
  describe "filters" do
    it "gets the columns and columns families for the filter" do
      get :filters, :table => "employee_super_mart"
      response.should render_template "filters"
    end
  end
  describe "create" do
    it "displays the results for a query" do
      get :create, :t => "employee_super_mart", :q => "employee.name:bob", :column_data => 
        ["family_department", "column_department_deptNo", "column_department_moreThanOneDepartment", "column_department_name"]
      response.should render_template "create"
    end
  end
end
