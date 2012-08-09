class Audit < ActiveRecord::Base
	def self.log_event(user, message, model, mutation)
    Audit.create(
      :user_id => user.id,
      :mutation => mutation.downcase,
      :model_affected => model.downcase,
      :action => "#{message} by #{user.username} (#{model})"
    )
	end
end
