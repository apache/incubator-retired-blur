module Js
  class Constants
    class << self
      def generate!(filename, namespace, constants_hash)
        constants = constants_hash.collect {|k,v| "#{k}:'#{v}'"}
        js = "var #{namespace} = {#{constants.join(",\n")}}"
        File.open("#{Rails.public_path}/javascripts/#{filename}", 'w') do |f|
          f.write js
        end
      end
    end
  end
  
  class Routes
    class << self
      def generate!(options = {})
        options[:file] ||= 'routes.js'
        functions = []
        functions << check_format_function
        functions << check_parameter_function
        named_routes = Rails.application.routes.named_routes
        Rails.application.routes.routes.each do |route|
          name = named_routes.routes.key(route).to_s
          next if name.blank?
          next if options[:except] && options[:except] === name
          next unless !options[:only] || options[:only] === name
          functions << "#{name}_path:function(#{build_params(route)}) {#{build_default_params(route)} return #{build_path(route)};}"
        end
        
        js = "var Routes = {#{functions.join(",\n")}}"
        File.open("#{Rails.public_path}/javascripts/#{options[:file]}", 'w') do |f|
          f.write js
        end
      end
      
      protected
        def build_params(route)
          s = []
          route.segment_keys.each do |seg|
            s << seg.to_s
          end
          s.join(',')
        end
        
        def build_default_params(route)
          s = []
          route.segment_keys.each do |seg|
            segg = seg.to_s
            if seg == :format
              s << "#{segg}=Routes._cf(#{segg});"
            else
              s << "#{segg}=Routes._cp(#{segg});"
            end
          end
          s.join('')
        end
        
        def build_path(route)
          s = "'#{ENV['RAILS_RELATIVE_URL_ROOT']}#{route.path.gsub(/\(|\)/,'')}'"
          route.segment_keys.each do |seg|
            if seg == :format
              s = s.gsub(Regexp.new("\.:#{seg}"), "' + #{seg} + '")
            else
              s = s.gsub(Regexp.new(":#{seg}"), "' + #{seg} + '")
            end
          end
          s.gsub(/ \+ ''/, '')
        end
        
        def check_format_function
          "_cf: function(param) { return (param === undefined) ? '' : ('.'+param); }"
        end
        
        def check_parameter_function
          "_cp: function(param) { return (param === undefined) ? '' : param; }"
        end
    end
  end
end
