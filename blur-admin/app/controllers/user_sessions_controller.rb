class UserSessionsController < ApplicationController
  skip_before_filter :current_zookeeper, :zookeepers

  def new
    @user_session = UserSession.new
  end

  def create
    @user_session = UserSession.new params[:user_session]
    if @user_session.save
      redirect_to root_url, :notice => "Successfully Logged In"
    else
      render :action => 'new'
    end
  end

  def destroy
    current_user_session.destroy
    redirect_to login_path, :notice => "Successfully Logged Out"
  end

end
