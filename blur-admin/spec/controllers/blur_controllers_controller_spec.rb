require 'spec_helper'

describe BlurControllersController do
  describe "actions" do
    before do
      # Universal setup
      setup_tests

      @blur_controller = FactoryGirl.create :blur_controller
      BlurController.stub!(:find).and_return @blur_controller
    end

    describe 'DELETE destroy' do
      before do
        @blur_controller.stub!(:destroy)
      end

      it "destroys the controller" do
        @blur_controller.should_receive(:destroy)
        delete :destroy, :id => @blur_controller.id, :format => :json
      end

      it "logs the event when the controller is deleted" do
        @blur_controller.stub!(:destroyed?).and_return true
        Audit.should_receive :log_event
        delete :destroy, :id => @blur_controller.id, :format => :json
      end
    end
  end
end
