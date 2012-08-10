class HdfsController < ApplicationController
  require 'hdfs_thrift_client'

  include ActionView::Helpers::NumberHelper

  def index
    @instances = Hdfs.all
    respond_to do |format|
      format.html
      format.json{render :json => @instances, :methods => [:most_recent_stats, :recent_stats]}
    end
  end

  def info
    @hdfs = Hdfs.find(params[:id]).hdfs_stats.last
    if @hdfs
      respond_to do |format|
        format.html{render :partial => 'info'}
      end
    else
      render :text => "<div>Stats for hdfs ##{params[:id]} not found, is the blur tools agent running?</div>"
    end
  end

  def folder_info
    client = build_client_from_id
    @path = params[:fs_path]
    @stat = client.stat @path
    respond_to do |format|
      format.html{render :partial => 'folder_info'}
    end
  end

  def slow_folder_info
    client = build_client_from_id
    @path = params[:fs_path]
    file_stats = client.ls(@path, true)
    @file_count = @folder_count = @file_size = 0
    file_stats.each do |stat|
      @file_size += stat.length
      @file_count += 1 unless stat.isdir
      @folder_count += 1 if stat.isdir
    end
    render :json => {:file_size=>number_to_human_size(@file_size),:file_count=>@file_count,:folder_count=>@folder_count}
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
    respond_to do |format|
      format.html{render :partial => 'expand'}
    end
  end

  def mkdir
    client = build_client_from_id
    path = "#{params[:fs_path]}/#{params[:folder]}/"
    path.gsub!(/\/\//, "/")
    client.mkdirs(path)
    Audit.log_event(current_user, "A folder, #{params[:folder]}, was created", "hdfs", "create")
    render :nothing => true
  end

  def file_info
    client = build_client_from_id
    @stat = client.stat params[:fs_path]
    respond_to do |format|
      format.html{render :partial => 'file_info'}
    end
  end

  def move_file
    client = build_client_from_id
    client.rename(params[:from], params[:to])
    file_name = params[:from].strip.split("/").last
    Audit.log_event(current_user, "File/Folder, #{file_name}, was moved or renamed to #{params[:to]}", "hdfs", "update")
    render :nothing => true
  end

  def delete_file
    client = build_client_from_id
    path = params[:path]
    client.delete path, true
    file_name = params[:path].strip.split("/").last
    Audit.log_event(current_user, "File/Folder, #{file_name}, was deleted", "hdfs", "delete")
    render :nothing => true
  end

  def upload_form
    render :partial => 'upload_form'
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
          Audit.log_event(current_user, "File, #{f.original_filename}, was uploaded", "hdfs", "create")
        end
      else
        @error = 'Problem with File Upload'
      end
    rescue Exception => e
      @error = e.to_s
    end
    render :partial => 'upload'
  end

  def file_tree
    client = build_client_from_id
    file_structure = client.folder_tree params[:fs_path], 4
    render :json => file_structure
  end

  private
  def build_client_from_id
    instance = Hdfs.find params[:id]
    HdfsThriftClient.client("#{instance.host}:#{instance.port}")
  end
end

