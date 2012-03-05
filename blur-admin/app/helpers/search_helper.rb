module SearchHelper
  def is_valid_search?(search)
    #get the table schema for the current table
    schema = @blur_table.schema

    #get the array of the checked columns from the search
    search_columns = {}

    #get all of the columns from the search and convert them to ruby objects
    search.column_object.each do |value|
      parts = value.split('_-sep-_')
      if parts[0] == 'column'
        search_columns[parts[1]] ||= []
        search_columns[parts[1]] << parts[2]
      end
    end

    #Set difference of search columns from schema columns
    search_columns.each do |family, columns|
      schema_family = schema.select{|v| v['name'] == family}.first
      schema_columns = schema_family.nil? ? [] : schema_family['columns'].collect{|v| v['name']}
      test_arr = columns - schema_columns
      if test_arr.length > 0
        return false
      end
    end

    #if every test above passes than this is valid
    true
  end
end
