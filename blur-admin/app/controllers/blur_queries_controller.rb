class BlurQueriesController < ApplicationController

  before_filter :current_zookeeper, :only => [:index, :refresh]

  def index
    @filters = current_user.filter_preference.value || {}
    filters = {}
    now = Time.now
    filters['created_at'] = (now - @filters['created_at_time'].to_i.minutes)..now
    @filters.each {|k, v| filters[k] = v unless v == '' or k == 'created_at_time' or k == 'refresh_period'}
    # convert string bools into real bools
    filters['super_query_on'] = true  if filters['super_query_on'] == 'true'
    filters['super_query_on'] = false if filters['super_query_on'] == 'false'
    filters.delete('refresh_period')
    filters.delete('created_at_time')

    @blur_tables = @current_zookeeper.blur_tables

    @blur_queries = BlurQuery.joins(:blur_table => :cluster).
                             where(:blur_table =>{:clusters => {:zookeeper_id => @current_zookeeper.id}}).
                             where(filters).
                             includes(:blur_table).
                             order("created_at DESC")
  end

  def refresh
    filters = {}
    # convert string bools into real bools
    params[:super_query_on] = true  if params[:super_query_on] == 'true'
    params[:super_query_on] = false if params[:super_query_on] == 'false'

    # filters for columns
    [:blur_table_id, :super_query_on, :state].each do |category|
      filters[category] = params[category] unless params[category] == nil or params[category] == ''
    end
    
    # filters for userid
    user_filter = params[:userid] || ''
    unknown_user = params[:unknown_user] == 'true'
    exclude_user = params[:exclude_user] == 'true'
    
    userids = user_filter.split(' ')
    user_where_clause = []
    user_where_params = []
    user_where_clause << "userid is #{exclude_user ? 'not' : ''} null" if unknown_user
    if userids.size == 1
      user_where_clause << "userid #{exclude_user ? '!' : ''}=?"
      user_where_params << user_filter
    elsif userids.size > 1
      user_where_clause << "userid #{exclude_user ? 'not' : ''} in (#{Array.new(userids.size, '?').join(',')})"
      user_where_params << userids
    end
        
    # filter for time
    now = Time.now
    past_time = params[:created_at_time] ? now - params[:created_at_time].to_i.minutes : now - 1.minutes
    filters[:created_at] = past_time..now

    # filter for refresh period
    unless params[:time_since_refresh].empty?
      previous_filter_time = now - params[:time_since_refresh].to_i.seconds
      filters[:updated_at] = previous_filter_time..now
    end

    # filter by zookeeper
    @blur_queries = BlurQuery.joins(:blur_table => :cluster).
                             where(:blur_table =>{:clusters => {:zookeeper_id => @current_zookeeper.id}}).
                             where(filters).
                             where([user_where_clause.join(exclude_user ? ' and ' : ' or '), user_where_params].flatten.compact).
                             includes(:blur_table).
                             order("created_at DESC")
    respond_to do |format|
      format.html {render @blur_queries}
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
    @blur_query = BlurQuery.includes(:blur_table).find(params[:id])
    respond_to do |format|
      format.html {render :partial => 'more_info', :locals => {:blur_query => @blur_query}}
    end
  end

  def times
    times = BlurQuery.find(params[:id]).times
    puts times
    respond_to do |format|
      format.html { render :partial => 'times', :locals => {:times => times} }
    end
  end
end
