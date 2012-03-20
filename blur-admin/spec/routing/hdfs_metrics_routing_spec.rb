require "spec_helper"

describe HdfsMetricsController do
  describe "routing" do
    it "base routes to #index" do
      get("/hdfs_metrics").should route_to(:controller => "hdfs_metrics", :action => "index")
    end

    it "disk routes to #stats" do
      put("/hdfs_metrics/1/stats").should route_to(:controller => "hdfs_metrics", :action => "stats", :id => "1")
    end
  end
end
