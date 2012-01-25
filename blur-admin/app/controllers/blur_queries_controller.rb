class BlurQueriesController < ApplicationController
  respond_to :html, :only => :index
  respond_to :json, :except => :index
  before_filter :current_zookeeper, :only => [:index, :refresh]
  before_filter :zookeepers, :only => [:index, :refresh]

  def index
  end

  def refresh
    queries = BlurQuery.where_zookeeper(@current_zookeeper.id).includes(:blur_table).all
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

  def times
    times = BlurQuery.find(params[:id]).times
    respond_to do |format|
      format.html { render :partial => 'times', :locals => {:times => times} }
    end
  end
  
  def long_running
    Time.zone = 'Eastern'
    @blur_queries = BlurQuery.where_zookeeper(params[:zookeeper_id]).where(:state => 0).where('created_at < date_sub(utc_timestamp(), interval 1 minute)')
    
    render :partial => 'long_running'
  end
end
