class PreferenceController < ApplicationController

  def save 
    col_save = Preference.find_by_user_id(current_user.id, :conditions => {:pref_type => :column})
    col_save = Preference.create(:name => "column", :pref_type => "column", :user_id => current_user.id) unless col_save
    
    col_save.value = params['columns'].to_json
    col_save.save
    
    render :nothing => true
  end
  
end
