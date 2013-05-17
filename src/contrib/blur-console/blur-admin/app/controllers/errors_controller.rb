class ErrorsController < ApplicationController
  respond_to :json, :html

  def error_404
    render :error_404, :status => :not_found
  end

  def error_500
    @error = env["action_dispatch.exception"]
    @status_code = ActionDispatch::ExceptionWrapper.new(env, @exception).status_code
    render :error_500, :status => 500
  end

  def error_422
    render :error_422, :status => 422
  end
end
