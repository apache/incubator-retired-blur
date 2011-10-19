class HdfsController < ApplicationController
  require 'hdfs_thrift_client'

  def index
    @instances = Hdfs.select 'id, name'
  end

  def info
    @hdfs = HdfsStat.where('hdfs_id = ?', params[:id]).order("created_at desc").first
    render :partial => 'info'
  end
  
  def expand
    @hdfs_id = params[:id]
    instance = Hdfs.find @hdfs_id
    @path = params[:fs_path] || '/'
    @path += '/' unless @path.last=='/'
    client = HdfsThriftClient.client(instance.host, instance.port)
    fileStats = client.ls(@path, true)

    @children = fileStats.collect do |stat|
      file_ending = stat.path.split('/').last
      {:name=> file_ending, :is_dir=>stat.isdir}
    end

    render :layout => false
  end
  
  def file_info
    instance = Hdfs.find params[:id]
    
    client = HdfsThriftClient.client(instance.host, instance.port)
    @stat = client.stat params[:fs_path]
    render :layout => false
  end
  
  def cut_file
    instance = Hdfs.find params[:hdfs]
    client = HdfsThriftClient.client(instance.host, instance.port)
    
    client.mv(params[:target], params[:location])
    render :nothing => true
  end
  
  def delete_file
    instance = Hdfs.find params[:hdfs]
    client = HdfsThriftClient.client(instance.host, instance.port)
    
    uri = URI.parse params[:fs_path]
    client.rm uri.path
    render :nothing => true
  end
end





