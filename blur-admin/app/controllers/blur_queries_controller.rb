class BlurQueriesController < ApplicationController
  respond_to :html, :only => :index
  respond_to :json, :except => :index
  before_filter :current_zookeeper, :only => [:index, :refresh]
  before_filter :zookeepers, :only => [:index, :refresh]

  def refresh
    puts params.inspect
    lower_range = Time.now - (params[:time_length].to_i * 60)
    queries = BlurQuery.where_zookeeper(@current_zookeeper.id)
      .where("blur_queries.updated_at > ?", lower_range)
      .includes(:blur_table).all
    query_summaries = queries.collect do |query| 
      summary = query.summary(current_user)
      summary[:action] = ''
      summary
    end
    
    render :json => {:aaData => query_summaries}.to_json
  end

  def update
    @blur_query = BlurQuery.find params[:id]
    if params[:cancel] == 'true'
      @blur_query.cancel
    end
    respond_to do |format|
      format.html {render :partial => 'blur_query', :locals => { :blur_query => @blur_query }}
    end
  end

  def more_info
    @blur_query = BlurQuery.includes(:blur_table).find(params[:id])
    respond_to do |format|
      format.html {render :partial => 'more_info', :locals => {:blur_query => @blur_query}}
    end
  end
end
