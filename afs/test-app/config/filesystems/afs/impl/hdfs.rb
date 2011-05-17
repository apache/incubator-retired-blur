require 'singleton'

module Afs
  module Impl
    class HDFS
      include Singleton
      
      def initialize
      end
      
      def display
        "HDFS"
      end
      
      def internal_name
        "hdfs"
      end
      
      def dir_expand(level_to_expand)
        
      end
      
      def dir_list(dir_to_list)
        
      end
      
      def dir_info
        
      end
      
      def file_info
        
      end
    end
  end
end