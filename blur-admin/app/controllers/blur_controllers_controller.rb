class BlurControllersController < ApplicationController
  load_and_authorize_resource
  
  respond_to :json

  def destroy
    @blur_controller.destroy
    Audit.log_event(current_user, "Controller (#{@blur_controller.node_name}) was forgotten",
                    "controller", "delete") if @blur_controller.destroyed?
    respond_with(@blur_controller)
  end
end