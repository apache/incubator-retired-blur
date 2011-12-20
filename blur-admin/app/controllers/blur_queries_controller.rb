class BlurQueriesController < ApplicationController

  before_filter :current_zookeeper, :only => [:index, :refresh]
  before_filter :zookeepers, :only => [:index, :refresh]

  def index
    @filters = current_user.filter_preference.value || {'super_query' => 'true', 'created_at_time' => 1.minute}
    now = Time.now
    super_query = true if @filters['super_query_on'] == 'true'
    super_query = false  if @filters['super_query_on'] == 'false'

    @blur_tables = @current_zookeeper.blur_tables.where('status = 4')
    @blur_queries = BlurQuery.where_zookeeper(@current_zookeeper.id).filter_on_time_range((now - @filters['created_at_time'].to_i.minutes)..now)
    
    # Add super query filter
    @blur_queries = @blur_queries.where(:super_query_on => super_query)
    
    # Add state filter
    @blur_queries = @blur_queries.where(:state => @filters['state']) unless @filters['state'].blank?
    @blur_queries
  end

  def refresh
    super_query = true  if params[:super_query_on] == 'true'
    super_query = false if params[:super_query_on] == 'false'
            
    # filter for time
    now = Time.now
    past_time = params[:created_at_time] ? now - params[:created_at_time].to_i.minutes : now - 1.minutes
    time_range = past_time..now

    # filter for refresh period
    unless params[:time_since_refresh].empty?
      previous_filter_time = now - params[:time_since_refresh].to_i.seconds
      time_range = previous_filter_time..now
    end

    # filter by zookeeper
    @blur_queries = BlurQuery.where_zookeeper(@current_zookeeper.id).filter_on_time_range(time_range)
    
    #filter by table
    @blur_queries = @blur_queries.where(:blur_table_id => params[:blur_table_id]) unless params[:blur_table_id].blank?
    
    # Add super query filter
    @blur_queries = @blur_queries.where(:super_query_on => super_query) unless super_query.nil?
    
    # Add state filter
    @blur_queries = @blur_queries.where(:state => params['state']) unless params['state'].blank?
    
    # Add user filter
    unless params[:userid].blank? || params[:users].blank?
      users = params[:userid].split(' ').compact
      users_without_unknown = users.reject{|u| u == 'unknown'}      
      user_params_without_unknown = Array.new(users_without_unknown.size, '?').join(',')
      
      where_clause = ""
      if params[:users] == 'only'
        where_parts = []
        if users.include? 'unknown'
          where_parts << 'userid is null'
        end
        if users_without_unknown.size > 0
          where_parts << "userid in (#{user_params_without_unknown})"
        end
        where_clause = where_parts.join(' or ')
      else
        where_parts = []
        if users.include? 'unknown'
          where_parts << 'userid is not null'
        end
        if users_without_unknown.size > 0
          where_parts << "userid not in (#{user_params_without_unknown})"
        end
        where_clause = where_parts.join(' and ')
      end
      @blur_queries = @blur_queries.where([where_clause, users_without_unknown].flatten)
    end

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
