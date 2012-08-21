class PreferencesController < ApplicationController
  skip_before_filter :zookeepers, :current_zookeeper

  def update
    @preference = Preference.find_by_pref_type_and_user_id params[:pref_type], params[:user_id]
    authorize! :update, @preference
    updated_attr = {:value => params['value']}
    
    # Zookeeper pref uses the name as a data store
    updated_attr[:name] = params['name'] if params[:pref_type] == 'zookeeper'
    @preference.try(:update_attributes, updated_attr)
    render :nothing => true
  end
end
