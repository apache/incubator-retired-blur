class Audit < ActiveRecord::Base
	def self.log_event(user, message, model, mutation)
    Audit.create(
      :user => user,
      :mutation => mutation.downcase,
      :model_affected => model.downcase,
      :action => "#{message} by #{user} (#{model})"
    )
	end
end
