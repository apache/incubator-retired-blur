require "spec_helper"

describe BlurQueriesController do
  describe "routing" do
    it "blurqueries routes to #index" do
      get("/zookeepers/1/blur_queries").should route_to(:controller => "blur_queries", :action => "index", :zookeeper_id => '1')
    end

    it "refresh routes to #refresh" do
      get("/zookeepers/1/blur_queries/refresh/1").should route_to(:controller => "blur_queries", :action => "refresh", :time_length => '1', :zookeeper_id => '1')
    end

    it "more_info routes to #more_info" do
      get("/zookeepers/1/blur_queries/1/more_info").should route_to(:controller => "blur_queries", :action => "more_info", :id => '1', :zookeeper_id => '1')
    end
  end
end