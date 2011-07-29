class PreferenceController < ApplicationController

  def save 
    col_save = Preference.find_or_create_by_user_id_and_pref_type_and_name(current_user.id, :pref_type => :columns, :name => :columns)
    col_save.value = params['columns'].to_json
    col_save.save
    
    render :nothing => true
  end
  
  def save_filters
    filters = { :created_at_time => params[:created_at_time],
                :super_query_on  => params[:super_query_on],
                :running         => params[:running],
                :interrupted     => params[:interrupted],
                :refresh_period  => params[:refresh_period]}
    filter_save = Preference.find_or_create_by_user_id_and_pref_type_and_name(current_user.id, :pref_type => :filters, :name => :filters, :value => filters.to_json)
    filter_save.value = filters.to_json
    filter_save.save
    
    render :nothing => true
  end
  
end
