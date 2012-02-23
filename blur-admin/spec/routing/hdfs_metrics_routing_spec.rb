require "spec_helper"

describe HdfsMetricsController do
  describe "routing" do

    it "routes to #index" do
      pending "Just wanted the example"
      get("/hdfs_metrics").should route_to("hdfs_metrics#index")
    end
  end
end
