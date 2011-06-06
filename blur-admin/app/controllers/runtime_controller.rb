class RuntimeController < ApplicationController
  before_filter :setup_thrift
  after_filter :close_thrift

  def show
    @tables = @client.tableList()
    table_name = params[:table_name]
    if table_name and table_name.downcase != 'all'
      @blur_queries = BlurQueries.where(:table_name => table_name.all)
    else
      @blur_queries = BlurQueries.all
    end

    respond_to do |format|
      format.html
      format.js
    end
  end

  def cancel
    @client.cancelQuery(params[:table], params[:uuid])
  end

end
