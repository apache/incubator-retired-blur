class SearchController < ApplicationController
  before_filter :current_zookeeper, :only => :show
  before_filter :zookeepers, :only => :show

  #Show action that sets instance variables used to build the filter column
  def show
    @blur_tables = @current_zookeeper.blur_tables.find(:all, :order => "table_name")
    @blur_table = @blur_tables.first
	  @columns = @blur_tables.first.schema["columnFamilies"] unless @blur_tables.empty?
    @searches = current_user.searches.reverse
	end

	#Filter action to help build the tree for column families
  def filters
    @blur_table = BlurTable.find params[:blur_table_id]
    begin
      @columns = @blur_table.schema["columnFamilies"]
    rescue NoMethodError
      @columns = []
    end
    #TODO render the new saved list
	  render '_filters.html.haml', :layout=>false
  end

	#Create action is a large action that handles all of the filter data
  #and either saves the data or performs a search
  def create
    #Otherwise perform a search
    #if the search_id param is set than the user is trying to directly run a saved query
    if params[:search_id]
      buff = Search.find params[:search_id]
    #else build a new search to be used for this specific search
    else
      drop = params[:column_data].first == "neighborhood_all"? params[:column_data].drop(1).to_json : params[:column_data].to_json
      params[:super_query] ? sq=true : sq=false
      buff = Search.new(:blur_table_id => params[:blur_table],
                        :super_query   => sq,
                        :columns       => drop,
                        :fetch         => params[:result_count].to_i,
                        :offset        => params[:offset].to_i,
                        :user_id       => current_user.id,
                        :query         => params[:query_string])
    end

    #use the model to begin building the blurquery
    @blur_table = BlurTable.find params[:blur_table]
    bq = buff.prepare_search


    # Parse out column family names from requested column families and columns
    column_data_val = JSON.parse buff.columns
    families = column_data_val.collect{|value|  value.split('_')[1] if value.starts_with?('family') }.compact
    columns = {}
    column_data_val.each do |value|
      parts = value.split('_')
      if parts[0] == 'column' and !families.include?(parts[1])
        parts = value.split('_')
        # possible TODO: block below can be replaced with: columns[parts[1]] ||= ['recordId']
        if (!columns.has_key?(parts[1]))
          columns[parts[1]] = []
          columns[parts[1]] << 'recordId'
        end
        columns[parts[1]] << parts[2]
      end
    end

    #reorder the CFs to use the preference
    preferences = current_user.saved_cols
    families = (preferences & families) | families

    #add the selectors that were just built to the blur query and retrieve the results
    sel = Blur::Selector.new
    sel.columnFamiliesToFetch = families unless families.blank?
    sel.columnsToFetch = columns unless columns.blank?
    bq.selector = sel
    bq.userId = current_user.username
    blur_results = BlurThriftClient.client.query(@blur_table.table_name, bq)

    #build a mapping from families to their associated columns
    families_with_columns = {}
    families.each do |family|
      families_with_columns[family] = ['recordId']
      @blur_table.schema['columnFamilies'][family].each do |column|
        families_with_columns[family] << column
      end
    end

    ####Dear lord this is scary####
    #this parses up the response object from blur and prepares it as a table
    visible_families = (families + columns.keys).uniq
    @results = []
    @result_count = blur_results.totalResults
    @result_time = blur_results.realTime
    blur_results.results.each do |result|
      row = result.fetchResult.rowResult.row
      max_record_count = row.columnFamilies.collect {|cf| cf.records.keys.count }.max

      # organize into multidimensional array of rows and columns
      table_rows = Array.new(max_record_count) { [] }
      (0...max_record_count).each do |record_count|
        visible_families.each do |column_family_name|
          column_family = row.columnFamilies.find { |cf| cf.family == column_family_name }
          table_rows[record_count] << cfspan = []

          families_include = families.include? column_family_name
          if column_family
            count = column_family.records.values.count
            if record_count < count
              if families_include
                families_with_columns[column_family_name].each do |column|
                  found_set = column_family.records.values[record_count].find { |col| column == col.name }
                    if !(column == 'recordId')
                      cfspan << (found_set.nil? ? ' ' : found_set.values.join(', '))
                    else
                      cfspan << column_family.records.keys[record_count]
                    end
                end
              else
                columns[column_family_name].each do |column|
                  found_set = column_family.records.values[record_count].find { |col| column == col.name }
                    if !(column == 'recordId')
                      cfspan << (found_set.nil? ? ' ' : found_set.values.join(', '))
                    else
                      cfspan << column_family.records.keys[record_count]
                    end
                end
              end
            else
              if families_include
                families_with_columns[column_family_name].count.times { |count_time| cfspan << ' ' }
              else
                columns[column_family_name].count.times { |count_time| cfspan << ' ' }
              end
            end
          else
            if families_include
              families_with_columns[column_family_name].count.times { |count_time| cfspan << ' ' }
            else
              columns[column_family_name].count.times { |count_time| cfspan << ' ' }
            end
          end
        end
      end

      record = {:id => row.id, :max_record_count => max_record_count, :row => row, :table_rows => table_rows}

      @results << record
    end

    @all_columns = families_with_columns.merge columns
    @column_names = @all_columns.values
    @family_names = @all_columns.keys

    render :template=>'search/create.html.haml', :layout => false
  end

  #save action that loads the state of a saved action and returns a json to be used to populate the form
  def load
    #TODO logic to check if the saved search is valid if it is render the changes to the page
    #otherwise change the state of the save and load what you can
    @search = Search.find params['search_id']
    render :json => {:saved => @search, :success => true }
  end

  #Delete action used for deleting a saved search from a user's saved searches
  def delete
    Search.find(params[:search_id]).delete
    @searches = current_user.searches.reverse
    @blur_table = BlurTable.find params[:blur_table]
    respond_to do |format|
      format.html {render :partial =>"saved.html.haml" }
    end
  end

  def reload
    @searches = current_user.searches.reverse
    @blur_table = BlurTable.find params[:blur_table]
    respond_to do |format|
      format.html {render :partial =>"saved.html.haml" }
    end
  end
  
  def save
    drop = params[:column_data].first == "neighborhood_all"? params[:column_data].drop(1).to_json : params[:column_data].to_json
    Search.create(:name          => params[:save_name],
                  :blur_table_id => params[:blur_table],
                  :super_query   => params[:super_query],
                  :columns       => drop,
                  :fetch         => params[:result_count].to_i,
                  :offset        => params[:offset].to_i,
                  :user_id       => current_user.id,
                  :query         => params[:query_string])
    @searches = current_user.searches.reverse
    @blur_table = BlurTable.find params[:blur_table]

    respond_to do |format|
      format.html {render :partial =>"saved.html.haml" }
    end
  end
end
