class PreferencesController < ApplicationController
  def update
    @user = User.find params[:user_id]
    @preference = @user.preferences.find_by_pref_type params[:pref_type]
    @preference.update_attributes :value => params['value']
    render :nothing => true
  end
end
