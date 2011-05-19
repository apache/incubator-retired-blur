class RuntimeController < ApplicationController
    require 'thrift/blur'
    def show
      client = setup_thrift
      @tables = client.tableList()
      close_thrift
    end

    def current_queries
      client = setup_thrift
      running_queries = client.currentQueries(params[:table])
      close_thrift

      render :json => running_queries
    end

  end

