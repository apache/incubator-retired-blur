require "spec_helper"

describe BlurTablesController do
  describe "routing" do
    it "base routes to #index" do
      get("/blur_tables").should route_to(:controller => "blur_tables", :action => "index")
    end

    it "enable routes to #enable" do
      put("/blur_tables/enable").should route_to(:controller => "blur_tables", :action => "enable")
    end

    it "disable routes to #disable" do
      put("/blur_tables/disable").should route_to(:controller => "blur_tables", :action => "disable")
    end

    it "forget routes to #forget" do
      delete("/blur_tables/forget").should route_to(:controller => "blur_tables", :action => "forget")
    end

    it "reload routes to #reload" do
      get("/blur_tables/reload").should route_to(:controller => "blur_tables", :action => "reload")
    end

    it "destroy routes to #destroy" do
      delete("/blur_tables").should route_to(:controller => "blur_tables", :action => "destroy")
    end

    it "host routes to #host" do
      get("/blur_tables/1/hosts").should route_to(:controller => "blur_tables", :action => "hosts", :id => '1')
    end

    it "schema routes to #schema" do
      get("/blur_tables/1/schema").should route_to(:controller => "blur_tables", :action => "schema", :id => '1')
    end

    it "terms routes to #terms" do
      post("/blur_tables/1/terms").should route_to(:controller => "blur_tables", :action => "terms", :id => '1')
    end
  end
end