class UsersController < ApplicationController

  load_and_authorize_resource

  before_filter :find_user, :only => [:show, :edit, :update, :destroy, :save]
  skip_before_filter :current_zookeeper, :zookeepers

  def index
    @users = User.all
  end

  def show
    @column_preference = @user.column_preference
    @choices = BlurTable.select('table_schema').collect {|table| schema = table.schema; schema.keys if schema}.flatten.uniq
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
    redirect_to users_path, :notice => "User Removed"
  end

  private

  def find_user
    @user = User.find params[:id]
  end
end
