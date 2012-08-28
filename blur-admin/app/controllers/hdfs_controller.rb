class HdfsController < ApplicationController
  require 'hdfs_thrift_client'
  load_and_authorize_resource :shallow => true

  HTML_REPONSES = [:index, :info, :folder_info, :expand, :file_info, :upload_form, :upload]
  respond_to :html, :only => HTML_REPONSES
  respond_to :json, :except => HTML_REPONSES

  include ActionView::Helpers::NumberHelper

  def index
    respond_with do |format|
      format.json{render :json => @hdfs, :methods => [:most_recent_stats, :recent_stats]}
    end
  end

  def info
    respond_with(@hdfs_stat) do |format|
      format.html do
        if @hdfs_stat
          render :partial => 'info'
        else
          render :text => "<div>Stats for hdfs ##{params[:id]} were not found, is the blur tools agent running?</div>"
        end
      end
    end
  end

  def folder_info
    client = build_client_from_id
    path = params[:fs_path]

    @stat = client.stat path
    respond_with(@stat) do |format|
      format.html{ render :partial => 'folder_info' }
    end
  end

  def slow_folder_info
    client = build_client_from_id
    path = params[:fs_path]
    file_stats = client.ls(@path, true)
    file_size = 0
    stats_hash = {:file_size => 0, :file_count => 0, :folder_count => 0}

    file_stats.each do |stat|
      file_size += stat.length
      stats_hash[:file_count] += 1 unless stat.isdir
      stats_hash[:folder_count] += 1 if stat.isdir
    end
    stats_hash[:file_size] = number_to_human_size file_size

    respond_with(stats_hash)
  end

  def expand
    @hdfs_id = params[:id]
    @path = params[:fs_path] || '/'
    @path += '/' unless @path.last=='/'
    client = build_client_from_id
    fileStats = client.ls(@path)

    @children = fileStats.collect do |stat|
      file_ending = stat.path.split('/').last
      {:name=> file_ending, :is_dir=>stat.isdir}
    end
    @children.sort_by! {|c| [c[:is_dir] ? 1:0, c[:name].downcase]}

    respond_with do |format|
      format.html { render :partial => 'expand' }
    end
  end

  def mkdir
    client = build_client_from_id
    path = "#{params[:fs_path]}/#{params[:folder]}/"
    path.gsub!(/\/\//, "/")
    client.mkdirs(path)
    Audit.log_event(current_user, "A folder, #{params[:folder]}, was created",
      "hdfs", "create", @hdfs)

    respond_with do |format|
      format.json { render :nothing => true }
    end
  end

  def file_info
    client = build_client_from_id
    @stat = client.stat params[:fs_path]
    respond_with do |format|
      format.html { render :partial => 'file_info' }
    end
  end

  def move_file
    client = build_client_from_id
    client.rename(params[:from], params[:to])
    file_name = params[:from].strip.split("/").last
    Audit.log_event(current_user, "File/Folder, #{file_name}, was moved or renamed to #{params[:to]}",
      "hdfs", "update", @hdfs)

    respond_with do |format|
      format.json { render :nothing => true }
    end
  end

  def delete_file
    client = build_client_from_id
    path = params[:path]
    client.delete path, true
    file_name = params[:path].strip.split("/").last
    Audit.log_event(current_user, "File/Folder, #{file_name}, was deleted",
      "hdfs", "delete", @hdfs)

    respond_with do |format|
      format.json { render :nothing => true }
    end
  end

  def upload_form
    respond_with do |format|
      format.html { render :partial => 'upload_form' }
    end
  end

  def upload
    begin
      if !(params[:upload].nil? or params[:path].nil? or params[:id].nil?)
        f = params[:upload]
        @path = params[:path]
        if f.tempfile.size > 26214400
          @error = 'Upload is Too Large.  Files must be less than 25Mb.'
        else
          client = build_client_from_id
          client.put(f.tempfile.path,@path + '/' + f.original_filename)
          Audit.log_event(current_user, "File, #{f.original_filename}, was uploaded",
            "hdfs", "create", @hdfs)
        end
      else
        @error = 'Problem with File Upload'
      end
    rescue Exception => e
      @error = e.to_s
    end
    respond_with do |format|
      format.html { render :partial => 'upload' }
    end
    
  end

  def file_tree
    client = build_client_from_id
    file_structure = client.folder_tree params[:fs_path], 4
    respond_with(file_structure)
  end

  private
  def build_client_from_id
    HdfsThriftClient.client("#{@hdfs.host}:#{@hdfs.port}")
  end
end

