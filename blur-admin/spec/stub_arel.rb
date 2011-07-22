class MockArel
	AREL_METHODS = [:where, :select, :group, :order, :limit, :offset, :joins, :includes, :lock, :readonly, :from]
	AREL_METHODS.each do |method|
		define_method method do |*args|
			self
		end
	end

	def initialize(clazz)
		@scopes = clazz.scopes.keys
	end

	def and_return(value)
		@value = value
		self
	end

	def method_missing(symbol, *args, &block)
		return self if @scopes.include?(symbol)
		if /^find_by/ === symbol.to_s
			return @value.first if @value.kind_of? Array
			return @value
		end
		@value.send symbol, *args, &block
	end
end

module StubArelMixin
	def self.included(base)
		base.extend ClassMethods
	end

	module ClassMethods
		def stub_arel
			mock_arel = MockArel.new(self)
			methods = (MockArel::AREL_METHODS + scopes.keys).flatten
			methods.each do |method|
				(class << self; self; end).class_eval do
					define_method method do |*args|
						mock_arel
					end
				end
			end
			mock_arel
		end
	end

	def stub_arel(method, clazz)
		mock_arel = MockArel.new(clazz)
		(class << self; self; end).class_eval do
			define_method method do |*args|
				return mock_arel
			end
		end
		mock_arel
	end
end

class Object
	include StubArelMixin
end
