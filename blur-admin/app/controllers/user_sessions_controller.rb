class UserSessionsController < ApplicationController
  respond_to :html

  def new
    respond_with(@user_session = UserSession.new)
  end

  def create
    @user_session = UserSession.new params[:user_session]
    flash[:notice] = "Successfully Logged In" if @user_session.save
    respond_with(@user_session, :location => root_url)
  end

  def destroy
    current_user_session.destroy
    reset_session
    redirect_to login_path, :notice => "Successfully Logged Out"
  end

end
