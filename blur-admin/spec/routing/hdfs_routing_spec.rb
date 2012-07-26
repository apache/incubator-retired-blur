require "spec_helper"

describe HdfsController do
  describe "routing" do
    it "base with nothing routes to #index" do
      get("/hdfs").should route_to(:controller => "hdfs", :action => "index")
    end

    it "base with id should route to #index" do
      get("/hdfs/1").should route_to(:controller => "hdfs", :action => "index", :id => '1')
    end

    it "base with id and show should route to #index" do
      get("/hdfs/1/show").should route_to(:controller => "hdfs", :action => "index", :id => '1')
    end

    it "base with id and show and a path should route to #index" do
      get("/hdfs/1/show/path").should route_to(:controller => "hdfs", :action => "index", :id => '1', :fs_path => '/path')
    end

    it "info routes to #info" do
      get("/hdfs/1/info").should route_to(:controller => "hdfs", :action => "info", :id => "1")
    end

    it "folderinfo routes to #folderinfo" do
      get("/hdfs/1/folder_info").should route_to(:controller => "hdfs", :action => "folder_info", :id => "1")
    end

    it "slowfolderinfo routes to #slowfolderinfo" do
      get("/hdfs/1/slow_folder_info").should route_to(:controller => "hdfs", :action => "slow_folder_info", :id => "1")
    end

    it "expand without path routes to #expand" do
      get("/hdfs/1/expand").should route_to(:controller => "hdfs", :action => "expand", :id => "1")
    end

    it "expand with path routes to #expand" do
      get("/hdfs/1/expand/path").should route_to(:controller => "hdfs", :action => "expand", :id => "1", :fs_path => '/path')
    end

    it "file_info without path routes to #file_info" do
      get("/hdfs/1/file_info").should route_to(:controller => "hdfs", :action => "file_info", :id => "1")
    end

    it "file_info with path routes to #file_info" do
      get("/hdfs/1/file_info/path").should route_to(:controller => "hdfs", :action => "file_info", :id => "1", :fs_path => '/path')
    end

    it "move routes to #move_file" do
      post("/hdfs/1/move").should route_to(:controller => "hdfs", :action => "move_file", :id => "1")
    end

    it "mkdir routes to #mkdir" do
      post("/hdfs/1/mkdir").should route_to(:controller => "hdfs", :action => "mkdir", :id => "1")
    end

    it "delete_file routes to #delete_file" do
      post("/hdfs/1/delete_file").should route_to(:controller => "hdfs", :action => "delete_file", :id => "1")
    end

    it "structure routes to #structure" do
      get("/hdfs/1/structure").should route_to(:controller => "hdfs", :action => "file_tree", :id => "1")
    end

    it "upload_form routes to #upload_form" do
      get("/hdfs/1/upload_form").should route_to(:controller => "hdfs", :action => "upload_form", :id => '1')
    end

    it "disk routes to #stats" do
      get("/hdfs/1/stats").should route_to(:controller => "hdfs", :action => "stats", :id => "1")
    end

    it "upload routes to #upload" do
      post("/hdfs/1/upload").should route_to(:controller => "hdfs", :action => "upload", :id => "1")
    end
  end
end