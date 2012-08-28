class PreferencesController < ApplicationController
  load_and_authorize_resource

  respond_to :json

  def update
    @preference = Preference.find_by_pref_type_and_user_id params[:pref_type], params[:user_id]
    updated_attr = {:value => params['value']}
    
    # Zookeeper pref uses the name as a data store
    updated_attr[:name] = params['name'] if params[:pref_type] == 'zookeeper'
    @preference.try(:update_attributes, updated_attr)
    respond_with(@preference) do |format|
      format.json { render :nothing => true }
    end
  end
end
