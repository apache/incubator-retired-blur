require "spec_helper"

describe DataController do
  describe "show" do
    it "renders the show template" do
      get :show
      response.should render_template "show"
    end
  end
  describe "update" do
    it "enables the table if enable is true" do
      #put :update, :enabled => 'true', :id => "employee_super_mart"
      #response.should render_template true
    end
    it "disbales the table if enable is false"
      #TODO: when disable is uncommented, write test
  end
  describe "destroy" do
    it "deletes a table from the list"
      #TODO: when delete table is uncommented, write test
  end
end
