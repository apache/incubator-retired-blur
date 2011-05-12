module Afs
  class FileSystemsController < ApplicationController
    def index      
      @file_systems = Impl.constants.collect {|x| Afs::Impl.const_get(x).instance}
    end
    
    def dir_expand
      fs = params[:fs]
      level = params[:level]
      
      file_systems = Impl.constants.collect {|x| Afs::Impl.const_get(x).instance}
      fs_impl = file_systems.find{|x| x.internal_name == fs}
      
      render :json => fs_impl.dir_expand(level)
    end
  end
end