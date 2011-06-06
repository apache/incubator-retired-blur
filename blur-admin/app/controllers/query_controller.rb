class QueryController < ApplicationController

  before_filter :setup_thrift
  after_filter :close_thrift

	def show
	  @tables = @client.tableList.sort!
	  @columns = @client.schema(@tables.first) unless @tables.blank?
	end
	
	def filters
	  table = params[:table]
	  @columns = @client.schema(table)
	  render '_filters.html.haml', :layout=>false
  end

	def create		
		# TODO: Add in fetch filter, paging, superQueryOn/Off
		
	  table = params[:t]
		bq = Blur::BlurQuery.new :queryStr => params[:q], :fetch => 25, :uuid => Time.now.to_i*1000+rand(1000)

    families = params[:column_data].collect{|value|  value.split('_')[1] if value.starts_with?('family') }.compact
    columns = {}
    params[:column_data].each do |value|
      parts = value.split('_')
      if parts[0] == 'column' and !families.include?(parts[1])
        parts = value.split('_')
        if (!columns.has_key?(parts[1]))
          columns[parts[1]] = []
        end
        columns[parts[1]] << parts[2]
      end
    end

		sel = Blur::Selector.new
		sel.columnFamiliesToFetch = families unless families.blank?
		sel.columnsToFetch = columns unless columns.blank?

		bq.selector = sel

		blur_results = @client.query(table, bq)

    families_with_columns = {}
    families.each do |family|
      families_with_columns[family] = @client.schema(table).columnFamilies[family]
    end

    visible_families = (families + columns.keys).uniq
    @results = []
    @result_count = blur_results.totalResults
    blur_results.results.each do |result|
      row = result.fetchResult.rowResult.row 
      max_record_count = row.columnFamilies.collect {|cf| cf.records.keys.count }.max

      # organize into multidimensional array of rows and columns
      table_rows = Array.new(max_record_count) { [] }
      (0...max_record_count).each do |i|
        visible_families.each do |columnFamilyName|
          columnFamily = row.columnFamilies.find { |cf| cf.family == columnFamilyName }
          table_rows[i] << cfspan = []
    
          if columnFamily
            count = columnFamily.records.values.count
            if i < count
              if families.include? columnFamilyName
                families_with_columns[columnFamilyName].each do |s|
                  found_set = columnFamily.records.values[i].find { |col| s == col.name }
                      cfspan << (found_set.nil? ? ' ' : found_set.values.join(', '))
                end
              else
                columns[columnFamilyName].each do |s|
                  found_set = columnFamily.records.values[i].find { |col| s == col.name }
                      cfspan << (found_set.nil? ? ' ' : found_set.values.join(', '))
                end
              end
            else    
              if families.include? columnFamilyName       
                families_with_columns[columnFamilyName].count.times { |t| cfspan << ' ' }
              else
                columns[columnFamilyName].count.times { |t| cfspan << ' ' }
              end
            end
          else
            if families.include? columnFamilyName       
              families_with_columns[columnFamilyName].count.times { |t| cfspan << ' ' }
            else
              columns[columnFamilyName].count.times { |t| cfspan << ' ' }
            end
          end
        end
      end
    
      record = {:id => row.id, :max_record_count => max_record_count, :row => row, :table_rows => table_rows}
      
      @results << record
    end
		
    @all_columns = families_with_columns.merge columns
		
		render :template=>'query/create.html.haml', :layout => false
	end

end