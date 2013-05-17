require "spec_helper"

describe BlurQueriesController do
  describe "routing" do
    it "blurqueries routes to #index" do
      get("/zookeepers/1/blur_queries").should route_to(:controller => "blur_queries", :action => "index", :zookeeper_id => '1')
    end

    it "refresh routes to #refresh as json" do
      get("/zookeepers/1/blur_queries/refresh/1.json").should route_to(:controller => "blur_queries", :action => "refresh", :time_length => '1', :zookeeper_id => '1', :format => 'json')
    end

    it "shoe routes to #show as html" do
      get("/blur_queries/1").should route_to(:controller => "blur_queries", :action => "show", :id => '1')
    end

    it "more_info routes to #show as json" do
      get("/blur_queries/1.json").should route_to(:controller => "blur_queries", :action => "show", :id => '1', :format => 'json')
    end

    it "cancel routes to #show as html" do
      put("/blur_queries/1/cancel.json").should route_to(:controller => "blur_queries", :action => "cancel", :id => '1', :format => 'json')
    end
  end
end