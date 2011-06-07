require "spec_helper"

describe DataController do
  describe "show" do
    it "renders the show template" do
      get :show
      response.should render_template "show"
    end
  end
  describe "update" do
    it "sets the enable checkbox" do

    end
  end
  describe "destroy" do
    it "deletes a table from the list" do
      
    end
  end
end
