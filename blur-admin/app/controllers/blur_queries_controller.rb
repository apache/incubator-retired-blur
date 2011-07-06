class BlurQueriesController < ApplicationController

  before_filter :current_zookeeper, :only => :index
  before_filter :zookeepers, :only => :index

  def index
    filters = {}
    # filters for columns
    [:blur_table_id, :super_query_on].each do |category|
      filters[category] = params[category] unless params[category] == nil or params[category] == ''
    end
    # filter for time
    past_time = params[:time] ? Time.zone.now - params[:time].to_i.minutes : Time.zone.now - 1.minutes
    filters[:created_at] = past_time..Time.zone.now
    @blur_tables = @current_zookeeper.blur_tables unless request.xhr?

    # below line introduces a ton of sql queries when filtering with @current_zookeeper
    @blur_queries = BlurQuery.all( :conditions => filters, :order => "created_at desc" ).keep_if { |blur_query| blur_query.zookeeper == @current_zookeeper }
    respond_to do |format|
      format.html do
        if request.xhr?
          render :partial => 'query_table'
        end
      end
    end
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
    @blur_query = BlurQuery.find(params[:id])
    respond_to do |format|
      format.html {render :partial => 'more_info', :locals => {:blur_query => @blur_query}}
    end
  end
end
