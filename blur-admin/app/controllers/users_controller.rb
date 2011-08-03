class UsersController < ApplicationController

  load_and_authorize_resource
  before_filter :find_user, :only => [:show, :edit, :update, :destroy, :save]
  skip_before_filter :current_zookeeper, :zookeepers

  def index
    @users = User.all
  end

  def show
    @column_preference = @user.column_preference
    @filter_preference = @user.filter_preference
    @columns = @user.column_preference.value
    @filters = @user.filter_preference.value
    @choices = BlurTable.all.collect {|table| table.schema.keys}.flatten.uniq 
  end

  def new
    @user = User.new
  end

  def create
    @user = User.new(params[:user])
    if @user.save
      redirect_to users_path, :notice => "User Created"
    else
      render 'new'
    end
  end

  def edit
  end

  def update
    if @user.update_attributes(params[:user])
      redirect_to users_path, :notice  => "User Updated"
    else
      render :action => 'edit'
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
