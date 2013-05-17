class AdminSettingsController < ApplicationController
  respond_to :html

  def update
    search_filter = AdminSetting.find_or_create_by_setting(params[:setting])
    search_filter.value = params[:value]
    search_filter.save
    respond_with do |format|
      format.html { render :nothing => true }
    end
  end
end