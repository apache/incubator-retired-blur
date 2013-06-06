require "spec_helper"

describe ZookeepersController do
  describe "routing" do
    it "base and zookeepers routes to #index" do
      get("/").should route_to(:controller => "zookeepers", :action => "index")
      get("/zookeepers").should route_to(:controller => "zookeepers", :action => "index")
    end

    it "index routes to #index as json" do
      get("/zookeepers.json").should route_to(:controller => "zookeepers", :action => "index", :format => 'json')
    end

    it "zookeepers with id routes to #show as json" do
      get("/zookeepers/1.json").should route_to(:controller => "zookeepers", :action => "show", :id => '1', :format => 'json')
    end

    it "delete zookeepers with id routes to #destroy" do
      delete("/zookeepers/1").should route_to(:controller => "zookeepers", :action => "destroy", :id => '1')
    end
  end
end