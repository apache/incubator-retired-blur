class RuntimeController < ApplicationController
  def show
    #TODO: Change @tables to populate from db once status is working
    @tables = BlurThriftClient.client.tableList
    table_name = params[:id]
    time = params[:time].to_i
    now_time = Time.zone.now
    if params[:time]
      past_time = Time.zone.now - time.minutes
    else
      past_time = Time.zone.now - 1.minutes
    end

    if table_name and table_name.downcase != 'all'
      @blur_queries = BlurQueries.where :table_name => table_name, :created_at => past_time..now_time
    else
      @blur_queries = BlurQueries.where :created_at => past_time..now_time
    end

    respond_to do |format|
      format.html
      format.js
    end
  end

  def update
    if params[:cancel]
      table_name = params[:table]
      uuid = params[:uuid]
      query = BlurQueries.find_by_table_name_and_uuid table_name, uuid
      result = query.cancel
    end
    render :json => result
  end
  
  def info
    @blur_query = BlurQueries.find_by_uuid params[:uuid]
    render :partial=>'expanded_blur_query', :layout => false
  end

  def create
    
  end

end
