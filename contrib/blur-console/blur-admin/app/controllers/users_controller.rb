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
class UsersController < ApplicationController
  load_and_authorize_resource

  before_filter :zookeepers, :only => :show

  respond_to :html

  def index
    @search_filter = AdminSetting.search_filter
    respond_with(@users)
  end

  def show
    @column_preference = @user.column_preference
    @zookeeper_preference = @user.zookeeper_preference
    @choices = BlurTable.select('table_schema').collect {|table| schema = table.schema; schema.collect{|familes| familes['name']} if schema}.flatten.uniq
    respond_with(@user)
  end

  def new
    @user = User.new
  end

  def create
    if @user.save
      if can? :index, :users
        redirect_to users_path, :notice => "User Created"
      else
        redirect_to @user, :notice => "User Created"
      end
    else
      render 'new'
    end
  end

  def update
    if @user.update_attributes(params[:user])
      Audit.log_event(current_user, "User, #{@user.username}, had their roles updated",
        "users", "update", current_zookeeper)
      if can? :index, :users
        redirect_to users_path, :notice => "User Updated"
      else
        redirect_to @user, :notice => "User Updated"
      end
    else
      render 'edit'
    end
  end

  def destroy
    @user.destroy
    Audit.log_event(current_user, "User, #{@user.username}, was removed",
      "users", "delete", current_zookeeper)
    flash[:notice] = "User Removed"
    respond_with(@user)
  end
end
