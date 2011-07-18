class UsersController < ApplicationController

  load_and_authorize_resource
  before_filter :find_user, :only => [:show, :edit, :update, :destroy, :save]
  skip_before_filter :current_zookeeper, :zookeepers

  def index
    @users = User.all
  end

  def show
    @tables = BlurTable.all
    @preferences = current_user.saved_cols
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
      render :action => 'new'
    end
  end

  def edit
  end

  def update
    puts params[:user]
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
  
  def save_column 
    col_save = Preference.find_by_user_id(current_user.id, :conditions => {:pref_type => :column})
    col_save = Preference.create(:name => "column", :pref_type => "column", :user_id => current_user.id) unless col_save
    
    col_save.value = params['columns'].to_json
    col_save.save
    
    render :nothing => true
  end

  private

  def find_user
    @user = User.find(params[:id])
  end
end
