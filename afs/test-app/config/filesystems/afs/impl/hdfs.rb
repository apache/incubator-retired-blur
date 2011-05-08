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
    end
  end
end