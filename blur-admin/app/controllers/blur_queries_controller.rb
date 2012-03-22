class BlurQueriesController < ApplicationController
  respond_to :html, :only => :index
  respond_to :json, :except => :index
  before_filter :set_zookeeper, :only => :index
  before_filter :current_zookeeper, :only => [:index, :refresh]
  before_filter :zookeepers, :only => [:index, :refresh]

  def refresh
    lower_range = params[:time_length].to_i.minute.ago
    queries = BlurTable.find_all_by_status(4).collect{ |table|
      table.blur_queries.where_zookeeper(@current_zookeeper.id).where("blur_queries.updated_at > ?", lower_range)
    }.flatten

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
    respond_to do |format|
      format.html{render :partial => 'blur_query'}
    end
  end

  def more_info
    @blur_query = BlurQuery.find(params[:id])
    respond_to do |format|
      format.html{render :partial => 'more_info'}
    end
  end
end
