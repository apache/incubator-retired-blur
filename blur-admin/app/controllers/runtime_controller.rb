class RuntimeController < ApplicationController
  after_filter :close_thrift, :only => [:show, :update]

  def show
    @tables = thrift_client.tableList
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
    cancel = params[:cancel]
    table_name = params[:table]
    uuid = params[:uuid]

    if cancel
      thrift_client.cancelQuery(table_name, uuid.to_i)
    end

    #TODO Change render so that it spits back a status of the cancel
    render :nothing => true
  end
  
  def info
    @uuid = params[:uuid]
    @result = (BlurQueries.where :uuid => @uuid)[0]
    puts @result.uuid
    render :template=>'runtime/_expanded_blur_query.html.haml', :layout => false
  end

end
