require "spec_helper"

describe HdfsMetricsController do
  describe "routing" do

    it "routes to #index" do
      get("/hdfs_metrics").should route_to("hdfs_metrics#index")
    end

    it "routes to #new" do
      get("/hdfs_metrics/new").should route_to("hdfs_metrics#new")
    end

    it "routes to #show" do
      get("/hdfs_metrics/1").should route_to("hdfs_metrics#show", :id => "1")
    end

    it "routes to #edit" do
      get("/hdfs_metrics/1/edit").should route_to("hdfs_metrics#edit", :id => "1")
    end

    it "routes to #create" do
      post("/hdfs_metrics").should route_to("hdfs_metrics#create")
    end

    it "routes to #update" do
      put("/hdfs_metrics/1").should route_to("hdfs_metrics#update", :id => "1")
    end

    it "routes to #destroy" do
      delete("/hdfs_metrics/1").should route_to("hdfs_metrics#destroy", :id => "1")
    end

  end
end
