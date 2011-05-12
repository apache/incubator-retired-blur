require 'singleton'

module Afs
  module Impl
    class Local
      include Singleton
      
      def initialize
      end

      def display
        "Local File System"
      end
      
      def internal_name
        "local"
      end
      
      def dir_expand(level_to_expand)
        level = level_to_expand == 'root' ? File.expand_path('~') : level_to_expand
        
        sub_dirs = {}
        Dir["#{level}/*/"].each {|x| sub_dirs[File.basename(x)] = x}
        
        sub_dirs
      end
      
      def dir_list
        
      end
      
      def dir_info
        
      end
      
      def file_info
        
      end
    end
  end
end