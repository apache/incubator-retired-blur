require "spec_helper"

describe RuntimeController do 
  describe "show" do
    it "renders the show template" do
      get :show
      response.should render_template "show"
    end
  end
  describe "update" do
    it "does not cancel a running query if cancel is false" do
      put :update, :cancel => false
      response.should render_template true
    end
    it "cancels a running query if cancel is true"
      #TODO: check that canceling query works
  end
end
