class BlurQueriesController < ApplicationController
  load_and_authorize_resource :through => :current_zookeeper, :shallow => true
  before_filter :zookeepers, :only => :index

  respond_to :html, :only => [:index, :show]
  respond_to :json, :only => [:refresh, :show, :cancel]

  def index
    respond_with(@blur_queries)
  end

  def refresh
    lower_range = params[:time_length].to_i.minute.ago
    query_summaries = @current_zookeeper.refresh_queries(lower_range).collect do |query| 
      summary = query.summary(current_user)
      summary[:action] = ''
      summary
    end

    respond_with(query_summaries) do |format|
      format.json { render :json => { :aaData => query_summaries } } # Root node aaData is for the datatable library
    end
  end

  def show
    respond_with(@blur_query) do |format|
      format.json { render :json => @blur_query.summary(current_user) }
    end
  end

  def cancel
    @blur_query.cancel
    Audit.log_event(current_user, "BlurQuery with UUID #{@blur_query.uuid}) was canceled",
                    "blur_query", "update")

    respond_with(@blur_query)
  end
end
