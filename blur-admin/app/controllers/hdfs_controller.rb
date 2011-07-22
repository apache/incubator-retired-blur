class HdfsController < ApplicationController

  require 'hdfs_thrift_client'

  def index
    temp_files
  end

  def files
    @file_name = params[:file]
    @file_names = []
    if temp_files.has_key?(@file_name)
      @file_names = temp_files[@file_name]
    end

   render :template=>'hdfs/files.html.haml', :layout => false
  end

  def temp_files
    @files = {'file1' => ['element1'],
      'file2' => ['element1', 'element2', 'element3'],
      'file3' => ['element1', 'element2']}
  end

end