require "spec_helper"

describe HdfsController do
  describe "routing" do
    it "base routes to #index" do
      get("/hdfs_metrics").should route_to(:controller => "hdfs_metrics", :action => "index")
    end

    it "disk routes to #disk_cap_usage" do
      put("/hdfs_metrics/1/disk").should route_to(:controller => "hdfs_metrics", :action => "disk_cap_usage", :id => "1")
    end

    it "nodes routes to #live_dead_nodes" do
      put("/hdfs_metrics/1/nodes").should route_to(:controller => "hdfs_metrics", :action => "live_dead_nodes", :id => "1")
    end

    it "blocks routes to #block_info" do
      put("/hdfs_metrics/1/block").should route_to(:controller => "hdfs_metrics", :action => "block_info", :id => "1")
    end
  end
end