# ./config/initializers/asset_precompile_prefix_fix.rb
#
# So we can deploy at a SubURI, and precompile assets to respect that with:
#    RAILS_RELATIVE_URL_ROOT=/foo rake assets:precompile
#
# See: http://stackoverflow.com/questions/7293918/broken-precompiled-assets-in-rails-3-1-when-deploying-to-a-sub-uri
#
# Confirmed working in Rails 3.1.3
# Future versions of Rails may make this monkey patch unneccesary. (or break
# it without making it unneccesary)
#
#
# We are monkey patching source originally (in 3.1.3) at:
# https://github.com/rails/rails/blob/master/actionpack/lib/sprockets/helpers/rails_helper.rb#L54

module Sprockets
  module Helpers
    module RailsHelper

      included do |klass|
        klass.alias_method_chain :asset_path, :prefix
      end
      
      def asset_path_with_prefix(*args)
        path = asset_path_without_prefix(*args)
        
        if !asset_paths.send(:has_request?)
          path = ENV['RAILS_RELATIVE_URL_ROOT'] + path if ENV['RAILS_RELATIVE_URL_ROOT']
        end
        
        return path
      end
       
                
    end 
  end
end