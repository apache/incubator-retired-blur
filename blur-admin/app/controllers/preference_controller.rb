class PreferenceController < ApplicationController

  def save 
    col_save = Preference.find_or_create_by_user_id_and_pref_type_and_name(current_user.id, :pref_type => :columns, :name => :columns)
    col_save.value = params['columns'].to_json
    col_save.save
    
    render :nothing => true
  end
  
  def save_filters
    filters = [ params[:created_at_time],
                params[:super_query_on],
                params[:running],
                params[:interrupted]]
    filter_save = Preference.find_or_create_by_user_id_and_pref_type_and_name(current_user.id, :pref_type => :filters, :name => :filters, :value => filters.to_json)
    filter_save.value = filters.to_json
    filter_save.save
    
    render :nothing => true
  end
  
end
