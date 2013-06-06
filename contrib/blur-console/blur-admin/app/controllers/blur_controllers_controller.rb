class BlurControllersController < ApplicationController
  load_and_authorize_resource
  
  respond_to :json

  def destroy
    raise "Cannot Remove A Controller that is online!" if @blur_controller.controller_status == 1
    @blur_controller.destroy
    Audit.log_event(current_user, "Controller (#{@blur_controller.node_name}) was forgotten",
                    "controller", "delete", @blur_controller.zookeeper) if @blur_controller.destroyed?
    respond_with(@blur_controller) do |format|
      format.json
    end
  end
end