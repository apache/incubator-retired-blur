class ControllersController < ApplicationController
  load_and_authorize_resource

  def destroy
    @controller.destroy
    Audit.log_event(current_user, "Controller (#{@controller.node_name}) was forgotten", "controller", "delete") if @controller.destroyed?

    respond_to do |format|
      format.html { render_404 }
      format.json { render :json => {} }
    end
  end
end