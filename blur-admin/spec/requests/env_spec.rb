require "spec_helper"

describe "Environment Status" do

  it "displays a widget for HDFS status" do
    get "/"
    response.should render_template(:show)
    response.body.should include("HDFS Status")
  end

end

