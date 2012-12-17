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
      put("/zookeepers/1/blur_tables/enable.json").should route_to(:controller => "blur_tables", :action => "enable", :zookeeper_id => '1', :format => 'json')
    end

    it "disable routes to #disable" do
      put("/zookeepers/1/blur_tables/disable.json").should route_to(:controller => "blur_tables", :action => "disable", :zookeeper_id => '1', :format => 'json')
    end

    it "destroy routes to #destroy" do
      delete("/zookeepers/1/blur_tables.json").should route_to(:controller => "blur_tables", :action => "destroy", :zookeeper_id => '1', :format => 'json')
    end

    it "terms routes to #terms" do
      get("/blur_tables/1/terms.json").should route_to(:controller => "blur_tables", :action => "terms", :id => '1', :format => 'json')
    end
  end
end