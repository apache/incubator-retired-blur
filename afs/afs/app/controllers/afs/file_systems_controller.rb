module Afs
  class FileSystemsController < ApplicationController
    def index      
      @file_systems = Impl.constants.collect {|x| Afs::Impl.const_get(x).instance}
    end
  end
end