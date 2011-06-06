class RuntimeController < ApplicationController
  before_filter :setup_thrift
  after_filter :close_thrift

  def show
    @tables = @client.tableList()
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
    cancel? table_name uuid = params[:cancel, :table, :uuid] 

    if cancel?
      @client.cancelQuery(table_name, uuid.to_i)
    end

    #TODO Change render so that it spits back a status of the cancel
    render :nothing => true
  end

end
