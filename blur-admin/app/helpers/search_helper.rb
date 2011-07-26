module SearchHelper
  def is_valid_search?(search)
    #get the table schema for the current table
    schema = @blur_table.schema

    #get the array of the checked columns from the search
    search_columns = {}

    #get all of the columns from the search and convert them to ruby objects
    search.raw_columns.each do |value|
      parts = value.split('_')
      if parts[0] == 'column'
        search_columns[parts[1]] ||= []
        search_columns[parts[1]] << parts[2]
      end
    end

    #Set difference of search columns from schema columns
    search_columns.each do |family, columns|
      schema[family] ||= []
      test_arr = columns - schema[family]
      if test_arr.length > 0
        return false
      end
    end

    #if every test above passes than this is valid
    true
  end
end
