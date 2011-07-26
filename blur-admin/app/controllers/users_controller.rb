class UsersController < ApplicationController

  load_and_authorize_resource
  before_filter :find_user, :only => [:show, :edit, :update, :destroy, :save]
  skip_before_filter :current_zookeeper, :zookeepers

  def index
    @users = User.all
  end

  def show
    @tables = BlurTable.all
    @preferences = JSON.parse(
      Preference.find_or_create_by_user_id_and_pref_type_and_name( current_user.id, 
                                                                    :pref_type => :columns, 
                                                                    :name => :columns, 
                                                                    :value => [].to_json).value)
    @filters = JSON.parse(
      Preference.find_or_create_by_user_id_and_pref_type_and_name(current_user.id, 
                                                                  :pref_type => :filters, 
                                                                  :name => :filters, 
                                                                  :value => [1, nil, nil, nil].to_json).value)
    
    @choices = []
    BlurTable.all.each do |table|
      @choices << table.schema["columnFamilies"].keys
    end
    @choices.flatten!.uniq!
  end

  def new
    @user = User.new
  end

  def create
    @user = User.new(params[:user])
    if @user.save
      redirect_to root_path, :notice => "Successfully Created User"
    else
      render 'new'
    end
  end

  def edit
  end

  def update
    if @user.update_attributes(params[:user])
      redirect_to @user, :notice  => "Successfully updated user."
    else
      render :action => 'edit'
    end
  end

  def destroy
    @user.destroy
    redirect_to users_path, :notice => "Successfully destroyed user."
  end

  private

  def find_user
    @user = User.find(params[:id])
  end
end
