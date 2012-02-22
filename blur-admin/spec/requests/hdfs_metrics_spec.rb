require 'spec_helper'

describe "HdfsMetrics" do
  describe "GET /hdfs_metrics" do
    it "works! (now write some real specs)" do
      # Run the generator again with the --webrat flag if you want to use webrat methods/matchers
      get hdfs_metrics_path
      response.status.should be(200)
    end
  end
end
