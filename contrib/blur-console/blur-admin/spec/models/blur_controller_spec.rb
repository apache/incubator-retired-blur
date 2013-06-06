require 'spec_helper'

describe BlurController do
  describe "as json" do
    it "should not have the date fields" do
      controller = FactoryGirl.create :blur_controller
      controller.as_json.should_not include("updated_at")
      controller.as_json.should_not include("created_at")
    end
  end
end