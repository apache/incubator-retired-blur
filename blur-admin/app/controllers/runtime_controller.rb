class RuntimeController < ApplicationController
  def show
    #TODO: Change @tables to populate from db once status is working
    @tables = BlurThriftClient.client.tableList
    table_name = params[:id]
    if table_name and table_name.downcase != 'all'
      @blur_queries = BlurQueries.find_all_by_table_name table_name
    else
      @blur_queries = BlurQueries.all
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
    
    #render :template=>'runtime/query_table.html.haml', :layout => false
  end

end
