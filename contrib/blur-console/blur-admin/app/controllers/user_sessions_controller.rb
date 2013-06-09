# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with this
# work for additional information regarding copyright ownership. The ASF
# licenses this file to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
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
