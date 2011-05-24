class DataController < ApplicationController

  require 'thrift/blur'
  def show
    client = setup_thrift
    bq = BG::BlurQuery.new
    bq.queryStr = '*'
		bq.fetch = 1
    bq.superQueryOn = false
    @tables = client.tableList()
    @tdesc = Hash.new
    @tschema = Hash.new
    @tserver = Hash.new
    @tcount = Hash.new
    #@tisEnabled = Hash.new
    @tables.each do |table|
      @tdesc[table] = client.describe(table)
      @tschema[table] = client.schema(table).columnFamilies
      @tserver[table] = client.shardServerLayout(table)
      @tcount[table] = client.query(table, bq).totalResults
      #@tisEnabled[table] = @tables.
    end
    close_thrift
  end

end
