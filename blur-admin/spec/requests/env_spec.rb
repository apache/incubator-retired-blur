require "spec_helper"

describe "Environment Status" do
  it "displays a widget for HDFS status, Zookeeper Status, and Blur Status" do
    get "/"
    response.should render_template(:show)
    response.body.should include("HDFS Status")
    response.body.should include("Zookeeper Status")
    response.body.should include("Blur Status")
  end
end

