class ErrorsController < ApplicationController
  
  respond_to :json, :html

  def error_404(exception = nil)
    if exception
      @not_found_path = exception.message 
    else
      @not_found_path = "The file"
    end
    render :error_404, :layout => false, :status => :not_found
  end

  def error_500(exception = nil)
    @error = exception if exception
    render :error_500, :layout => false, :status => 500
  end

  def error_422(exception = nil)
    render :error_422, :layout => false, :status => 422
  end
end
