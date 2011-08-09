class HdfsController < ApplicationController

  require 'hdfs_thrift_client'

  def index
    if Hdfs.all.length > 0
      hdfs_ids = Hdfs.select 'id'
      @files = {}
      @connections = {}
      hdfs_ids.each do |hdfs_id|
        @hdfs = HdfsThriftClient.client(hdfs_id.id)
        @hdfs.ls('/').each do |file|
          @files[file] = get_files file
          @connections[file] = hdfs_id.id
        end
      end
    end
  end

  def files
    hdfs = HdfsThriftClient.client(params[:connection])
    file = params[:file].gsub(/[ *]/, ' ' => '/', '*' => '.')
    file_names_hash = {}

    if hdfs.exists? file
      file_names = hdfs.ls file
      if file_names.length == 1 and file == file_names[0]
        file_names.clear
      end
      file_names.each do |name|
        file_names_hash[name] = hdfs.stat name
      end
    end

   render :template=>'hdfs/files.html.haml', :layout => false, :locals => {:connection => params[:connection], :file_names_hash => file_names_hash}
  end

  def get_files curr_file
    curr_file_children_hash = {}
    if @hdfs.exists? curr_file
      curr_file_children = @hdfs.ls curr_file
      curr_file_children.each do |child|
        if !curr_file.eql? child
          curr_file_children_hash[child] = get_files child
        end
      end
    end
    curr_file_children_hash
  end

  def search
    file_names_hash = {}
    connections = {}
    search_string = ""
    if params[:results]
      params[:results][0].each do |name, connection|
        if name == "*search_string*"
          search_string = connection
        else
          hdfs = HdfsThriftClient.client(connection.to_i)
          file_names_hash[name] = hdfs.stat name
          connections[name] = connection
        end
      end
    end
    respond_to do |format|
      format.html {render :partial => 'search_results', :locals => {:search_string => search_string, :connections => connections,:file_names_hash => file_names_hash}}
    end
  end
end