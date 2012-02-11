class UserSessionsController < ApplicationController
  def new
    @user_session = UserSession.new
  end

  def create
    @user_session = UserSession.new params[:user_session]
    if @user_session.save
      redirect_to root_url, :notice => "Successfully Logged In"
    else
      render 'new'
    end
  end

  def destroy
    current_user_session.destroy
    reset_session
    redirect_to login_path, :notice => "Successfully Logged Out"
  end

end
