require "spec_helper"

describe RuntimeController do 
  describe "show" do
    it "renders the show template" do
      get :show
      response.should render_template "show"
    end
  end
  describe "update" do
    it "cancels a running query if cancel is true" do
      
    end
  end
end
