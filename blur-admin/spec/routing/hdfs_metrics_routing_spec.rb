require "spec_helper"

describe HdfsMetricsController do
  describe "routing" do
    it "base routes to #index" do
      get("/hdfs_metrics").should route_to(:controller => "hdfs_metrics", :action => "index")
    end
  end
end
