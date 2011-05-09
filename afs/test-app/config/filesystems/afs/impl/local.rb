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
    end
  end
end