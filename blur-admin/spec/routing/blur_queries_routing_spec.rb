require "spec_helper"

describe BlurQueriesController do
  describe "routing" do
    it "blurqueries routes to #index" do
      get("/blur_queries").should route_to(:controller => "blur_queries", :action => "index")
    end

    it "refresh routes to #refresh" do
      get("/blur_queries/refresh/1").should route_to(:controller => "blur_queries", :action => "refresh", :time_length => '1')
    end

    it "more_info routes to #more_info" do
      get("/blur_queries/1/more_info").should route_to(:controller => "blur_queries", :action => "more_info", :id => '1')
    end

    it "times routes to #times" do
      get("/blur_queries/1/times").should route_to(:controller => "blur_queries", :action => "times", :id => '1')
    end
  end
end