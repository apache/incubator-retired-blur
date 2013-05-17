require "spec_helper"

describe AuditsController do
  describe "routing" do
    it "index routes to #index as html" do
      get("/audits").should route_to(:controller => "audits", :action => "index")
    end
    it "index routes to #index as json" do
      get("/audits.json").should route_to(:controller => "audits", :action => "index", :format => 'json')
    end
  end
end