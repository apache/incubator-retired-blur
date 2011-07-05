class SearchController < ApplicationController

	def show
    @blur_tables = BlurTable.all
	  @columns = @blur_tables.first.schema["columnFamilies"] unless @blur_tables.empty?
    @searches = @current_user.searches.reverse
	end

	def filters
    blur_table = BlurTable.find params[:blur_table_id]
    begin
      @columns = blur_table.schema["columnFamilies"]
    rescue NoMethodError
      @columns = []
    end
	  render '_filters.html.haml', :layout=>false
  end

	def create
		# TODO: Add in fetch filter

    @blur_table = BlurTable.find params[:blur_table]
    table = @blur_table.table_name
		bq = Blur::BlurQuery.new :queryStr => params[:query_string], :fetch => params[:result_count].to_i, :start => params[:offset].to_i, :uuid => Time.now.to_i*1000+rand(1000)
    if !params[:super_query]
      bq.superQueryOn = false
    end

    column_data_val = params[:column_data]
    # Parse out column family names from requested column families and columns
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

		sel = Blur::Selector.new
		sel.columnFamiliesToFetch = families unless families.blank?
		sel.columnsToFetch = columns unless columns.blank?

		bq.selector = sel

		blur_results = BlurThriftClient.client.query(table, bq)

    families_with_columns = {}
    families.each do |family|
      families_with_columns[family] = ['recordId']
      @blur_table.schema['columnFamilies'][family].each do |column|
        families_with_columns[family] << column
      end
    end

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

  def load
    #TODO logic to check if the saved search is valid if it is render the changes to the page
    #otherwise change the state of the save and load what you can
    render 'show_save.coffee.erb'
  end
end
