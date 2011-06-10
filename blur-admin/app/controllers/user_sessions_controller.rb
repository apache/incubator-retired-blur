class UserSessionsController < ApplicationController

  def new
    @user_session = UserSession.new
  end

  def create
    puts params[:user_session]
    @user_session = UserSession.new params[:user_session]
    if @user_session.save
      redirect_to root_url, :notice => "Successfully logged in." 
    else
      render :action => 'new'
    end
  end

  def destroy
    @user_session = UserSession.find
    @user_session.destroy
    redirect_to root_url, :notice => "Successfully logged out."
  end

end
