class PreferencesController < ApplicationController
  skip_before_filter :zookeepers, :current_zookeeper
  def update
    preference = Preference.find_by_pref_type_and_user_id params[:pref_type], params[:user_id]
    authorize! :update, preference
    preference.try(:update_attributes, :value => params['value'])
    render :nothing => true
  end
end
