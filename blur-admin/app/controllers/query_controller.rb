class QueryController < ApplicationController

	def show
	  @tables = BlurThriftClient.client.tableList.sort!
	  @columns = BlurThriftClient.client.schema(@tables.first) unless @tables.blank?
	end
	
	def filters
	  @columns = BlurThriftClient.client.schema(params[:table])
	  render '_filters.html.haml', :layout=>false
  end

	def create
		# TODO: Add in fetch filter, paging, superQueryOn/Off
		
	  table = params[:t]
		bq = Blur::BlurQuery.new :queryStr => params[:q], :fetch => params[:r].to_i, :uuid => Time.now.to_i*1000+rand(1000)
    if !params[:s]
      bq.superQueryOn = false
    end

    column_data_val = params[:column_data]
    families = column_data_val.collect{|value|  value.split('_')[1] if value.starts_with?('family') }.compact
    columns = {}
    column_data_val.each do |value|
      parts = value.split('_')
      if parts[0] == 'column' and !families.include?(parts[1])
        parts = value.split('_')
        if (!columns.has_key?(parts[1]))
          columns[parts[1]] = []
        end
        columns[parts[1]] << parts[2]
      end
    end

		sel = Blur::Selector.new #:columnFamiliesToFetch => families, :columnsToFetch => columns
		sel.columnFamiliesToFetch = families unless families.blank?
		sel.columnsToFetch = columns unless columns.blank?

		bq.selector = sel

		blur_results = BlurThriftClient.client.query(table, bq)

    families_with_columns = {}
    families.each do |family|
      families_with_columns[family] = BlurThriftClient.client.schema(table).columnFamilies[family]
    end

    visible_families = (families + columns.keys).uniq
    @results = []
    @result_count = blur_results.totalResults
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
                      cfspan << (found_set.nil? ? ' ' : found_set.values.join(', '))
                end
              else
                columns[column_family_name].each do |column|
                  found_set = column_family.records.values[record_count].find { |col| column == col.name }
                      cfspan << (found_set.nil? ? ' ' : found_set.values.join(', '))
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
		
		render :template=>'query/create.html.haml', :layout => false
	end

end
