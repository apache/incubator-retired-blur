class HdfsController < ApplicationController

  require 'hdfs_thrift_client'

  def index
    temp_files
  end

  def files
    @file_name = params[:file]
    @file_names = params[:files].split(',') if params[:files]


   render :template=>'hdfs/files.html.haml', :layout => false
  end

  def jstree
    data =  {
              :data => { :title =>'root', :attr => {:id => 'root'} },
              :children => [
                {
                  :data => { :title =>'file1', :attr => {:id => 'file1'} },
                  :children => ['element1']
                },
                {
                  :data => { :title =>'file2', :attr => {:id => 'file2'} },
                  :children => ['element1','element2','element3']
                },
                {
                  :data => { :title =>'file3', :attr => {:id => 'file3'} },
                  :children => ['element1','element2']
                }
              ]
            }
    puts data.to_json().inspect()

    respond_to do |format|
      format.json { render :json => data.to_json() }
    end
  end

  def temp_files
    @files = {
      'root1' => {
        'file1' => {'element1' => {}},
        'file2' => {'element2' => {}, 'element3' => {}, 'element4' => {}},
        'file3' => {'element5' => {}, 'element6' => {}} },
      'root2' => {
        'file4' => {'element7' => {'element8' => {'element9' => {'element10' => {}}}}},
        'file5' => {'element11' => {}, 'element12' => {}, 'element13' => {}},
        'file6' => {'element14' => {}, 'element15' => {}}
        }
      }
  end

end