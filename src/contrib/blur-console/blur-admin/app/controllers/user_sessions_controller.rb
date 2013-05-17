class UserSessionsController < ApplicationController
  respond_to :html

  def new
    respond_with(@user_session = UserSession.new)
  end

  def create
    @user_session = UserSession.new params[:user_session]
    saved = @user_session.save
    flash[:notice] = "Successfully Logged In" if saved
    # Redirects to path on save otherwise it goes to login
    respond_with(@user_session, :location => login_path) do |format|
      # Hack to fork the location on error and success
      format.html { redirect_to root_path } if saved
    end
  end

  def destroy
    current_user_session.destroy
    reset_session
    redirect_to login_path, :notice => "Successfully Logged Out"
  end

end
