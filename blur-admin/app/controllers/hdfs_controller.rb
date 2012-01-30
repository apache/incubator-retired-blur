class HdfsController < ApplicationController
  require 'hdfs_thrift_client'

  include ActionView::Helpers::NumberHelper
  
  def index
    @instances = Hdfs.select 'id, name'
  end

  def info
    @hdfs = HdfsStat.where('hdfs_id = ?', params[:id]).order("created_at desc").first
    if @hdfs
      render :partial => 'info'
    else
      render :text => "<div>Stats for hdfs ##{params[:id]} not found, is the blur tools agent running?</div>"
    end
  end

  def folder_info
    instance = Hdfs.find params[:id]
    client = HdfsThriftClient.client("#{instance.host}:#{instance.port}")
    @path = params[:fs_path]
    @stat = client.stat @path
    render :layout => false
  end

  def slow_folder_info
    instance = Hdfs.find params[:id]
    client = HdfsThriftClient.client("#{instance.host}:#{instance.port}")
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
    instance = Hdfs.find @hdfs_id
    @path = params[:fs_path] || '/'
    @path += '/' unless @path.last=='/'
    client = HdfsThriftClient.client("#{instance.host}:#{instance.port}")
    fileStats = client.ls(@path)

    @children = fileStats.collect do |stat|
      file_ending = stat.path.split('/').last
      {:name=> file_ending, :is_dir=>stat.isdir}
    end

    render :layout => false
  end

  def mkdir
    instance = Hdfs.find params[:id]
    client = HdfsThriftClient.client("#{instance.host}:#{instance.port}")
    path = "#{params[:fs_path]}/#{params[:folder]}/"
    path.gsub!(/\/\//, "/")
    client.mkdir(path)
    render :nothing => true
  end

  def file_info
   instance = Hdfs.find params[:id]
   client = HdfsThriftClient.client("#{instance.host}:#{instance.port}")
   @stat = client.stat params[:fs_path]
   render :layout => false
  end
  
  def move_file
    instance = Hdfs.find params[:id]
    client = HdfsThriftClient.client("#{instance.host}:#{instance.port}")
    
    puts params[:from]
    puts params[:to]
    
    client.rename(params[:from], params[:to])
    render :nothing => true
  end
  
  def delete_file
    instance = Hdfs.find params[:id]
    client = HdfsThriftClient.client("#{instance.host}:#{instance.port}")
    
    path = params[:path]
    client.delete path, true
    render :nothing => true
  end
  
  def upload_form
    render :layout => false
  end
  
  def upload
    begin
      if defined? params[:upload] and defined? params[:path] and defined? params[:hdfs_id]
        f = params[:upload]
        @path = params[:path]
        if f.tempfile.size > 26214400
          @error = 'Upload is Too Large.  Files must be less than 25Mb.'
        else
          instance = Hdfs.find params[:hdfs_id]
          client = HdfsThriftClient.client("#{instance.host}:#{instance.port}")
          client.put(f.tempfile.path,@path + '/' + f.original_filename)
        end
      else
        @error = 'Problem with File Upload'
      end
    rescue Exception => e
      @error = e.to_s
    end
    render :layout => false
  end
end





