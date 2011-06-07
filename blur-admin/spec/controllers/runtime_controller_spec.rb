require "spec_helper"

describe RuntimeController do 
  describe "show" do
    it "renders the show template" do
      get :show
      response.should render_template "show"
    end
  end
end
