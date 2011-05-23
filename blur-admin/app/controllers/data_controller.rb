class DataController < ApplicationController

  require 'thrift/blur'
  def show
    client = setup_thrift
    @tables = client.tableList()
    @tdesc = Hash.new
    @tschema = Hash.new
    @tables.each do |table|
      @tdesc[table] = client.describe(table)
      @tschema[table] = client.schema(table).columnFamilies
    end
    close_thrift
  end

end
