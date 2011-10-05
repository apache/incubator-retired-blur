class HdfsController < ApplicationController
  require 'hdfs_thrift_client'

  def index
    instances = Hdfs.select 'id, name'
    @tree_data = instances.collect {|hdfs| {:data => hdfs.name, :state => 'closed', :attr => {:class => 'hdfs_root'}, :metadata => {:hdfs_id => hdfs.id, :fs_path=>'/'}}}.to_json
  end
  
  def expand
    instance = Hdfs.find params[:hdfs]
    
    client = HdfsThriftClient.client(instance.host, instance.port)
    files = client.ls params[:fs_path]

    children_data = files.collect do |file|
      uri = URI.parse file
      stats = client.stat uri.path
      if stats.isdir
        {:data => uri.path.split('/').last, :state => 'closed', :attr => {:class => 'hdfs_dir'}, :metadata => {:hdfs_id => instance.id, :fs_path => uri.path}}
      else
        {:data => {:title => uri.path.split('/').last}, :attr => {:class => 'hdfs_file'}, :metadata => {:hdfs_id => instance.id, :fs_path => uri.path}, :children => nil}
      end
    end
    
    render :json => children_data
  end
  
  def view_node
    instance = Hdfs.find params[:hdfs]
    
    client = HdfsThriftClient.client(instance.host, instance.port)
    @stat = client.stat params[:fs_path]
    @files = client.ls(params[:fs_path], true) if @stat.isdir
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





