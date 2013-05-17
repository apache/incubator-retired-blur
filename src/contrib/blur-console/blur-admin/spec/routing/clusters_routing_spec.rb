require "spec_helper"

describe ClustersController do
  describe "routing" do
    it "destroy should route to #destroy as json" do
      delete("/clusters/1.json").should route_to(:controller => "clusters", :action => "destroy", :format => 'json', :id => '1')
    end
  end
end