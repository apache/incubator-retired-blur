require "spec_helper"

describe BlurControllersController do
  describe "routing" do
    it "index routes to #index as json" do
      delete("/blur_controllers/1.json").should route_to(:controller => "blur_controllers", :action => "destroy", :format => 'json', :id => '1')
    end
  end
end