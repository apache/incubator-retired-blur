class BlurQueriesController < ApplicationController

  def index
    filters = {}
    # filters for columns
    [:blur_table_id, :super_query_on].each do |category|
      filters[category] = params[category] unless params[category] == nil or params[category] == ''
    end
    # filter for time
    past_time = params[:time] ? Time.zone.now - params[:time].to_i.minutes : Time.zone.now - 1.minutes
    filters[:created_at] = past_time..Time.zone.now
    @blur_tables = BlurTable.all
    @blur_queries = BlurQuery.where filters
    respond_to do |format|
      format.html do
        if request.xhr?
          render :partial => 'query_table'
        end
      end
    end
  end

  def update
    @blur_query = BlurQuery.find_by_id params[:id]
    if params[:cancel]
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
