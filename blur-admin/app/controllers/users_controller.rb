class UsersController < ApplicationController
  load_and_authorize_resource

  before_filter :zookeepers, :only => :show

  respond_to :html

  def index
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
      Audit.log_event(current_user, "User, #{@user.username}, had their roles updated", "users", "update")
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
    Audit.log_event(current_user, "User, #{@user.username}, was removed", "users", "delete")
    flash[:notice] = "User Removed"
    respond_with(@user)
  end
end
