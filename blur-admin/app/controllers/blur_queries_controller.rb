class BlurQueriesController < ApplicationController
  respond_to :html, :only => :index
  respond_to :json, :except => :index
  before_filter :current_zookeeper, :only => [:index, :refresh]
  before_filter :zookeepers, :only => [:index, :refresh]

  def refresh
    lower_range = params[:time_length].to_i.minute.ago
    queries = BlurQuery.where_zookeeper(@current_zookeeper.id).where("blur_queries.updated_at > ?", lower_range)
    query_summaries = queries.collect do |query| 
      summary = query.summary(current_user)
      summary[:action] = ''
      summary
    end
    render :json => {:aaData => query_summaries}.to_json
  end

  def update
    @blur_query = BlurQuery.find params[:id]
    @blur_query.cancel if params[:cancel] == 'true'
    render :partial => 'blur_query'
  end

  def more_info
    @blur_query = BlurQuery.find(params[:id])
    render :partial => 'more_info'
  end
end
