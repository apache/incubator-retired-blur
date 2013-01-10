class ErrorsController < ApplicationController
  
  respond_to :json, :html

  def error_404(exception = nil)
    render :error_404, :status => :not_found
  end

  def error_500(exception = nil)
    @error = exception if exception
    render :error_500, :status => 500
  end

  def error_422(exception = nil)
    render :error_422, :status => 422
  end
end
