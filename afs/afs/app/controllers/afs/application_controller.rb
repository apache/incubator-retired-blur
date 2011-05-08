module Afs
  class ApplicationController < ::ApplicationController
    layout 'afs'
    
    protected
    self.config.asset_path = lambda {|asset| "/afs/public#{asset}"}
  end
end