require "spec_helper"

describe BlurTablesController do
  describe "routing" do
    it "base routes to #index" do
      get("/zookeepers/1/blur_tables").should route_to(:controller => "blur_tables", :action => "index", :zookeeper_id => '1')
    end

    it "json index routes to #index" do
      get("/zookeepers/1/blur_tables.json").should route_to(:controller => "blur_tables", :action => "index", :zookeeper_id => '1', :format => 'json')
    end

    it "enable routes to #enable" do
      put("/zookeepers/1/blur_tables/enable").should route_to(:controller => "blur_tables", :action => "enable", :zookeeper_id => '1')
    end

    it "disable routes to #disable" do
      put("/zookeepers/1/blur_tables/disable").should route_to(:controller => "blur_tables", :action => "disable", :zookeeper_id => '1')
    end

    it "destroy routes to #destroy" do
      delete("/zookeepers/1/blur_tables").should route_to(:controller => "blur_tables", :action => "destroy", :zookeeper_id => '1')
    end

    it "terms routes to #terms" do
      get("/zookeepers/1/blur_tables/1/terms").should route_to(:controller => "blur_tables", :action => "terms", :id => '1', :zookeeper_id => '1')
    end
  end
end