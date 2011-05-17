module Afs
  class FileSystemsController < ApplicationController
    def index      
      @file_systems = Impl.constants.collect {|x| Afs::Impl.const_get(x).instance}
      mod = Afs
    end
    
    def  view
       @file_systems = Impl.constants.collect {|x| Afs::Impl.const_get(x).instance}
      @file_system = @file_systems.find{|x| x.internal_name == params[:fs]}
      @root_dir_subs = @file_system.dir_expand("root")
    end

    def dir_expand
      fs = params[:fs]
      level = params[:level]

      file_systems = Impl.constants.collect {|x| Afs::Impl.const_get(x).instance}
      fs_impl = file_systems.find{|x| x.internal_name == fs}
      
      render :json => fs_impl.dir_expand(level)
    end
    
    def dir_list
      fs = params[:fs]
      level = params[:level]

      file_systems = Impl.constants.collect {|x| Afs::Impl.const_get(x).instance}
      fs_impl = file_systems.find{|x| x.internal_name == fs}
      
      render :json => fs_impl.dir_list(level)
    end
  end
end