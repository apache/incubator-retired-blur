class Audit < ActiveRecord::Base
  belongs_to :user

  scope :recent, lambda { |from, to|
    where(:created_at => from.hours.ago..to.hours.ago).
    includes(:user)
  }

	def self.log_event(user, message, model, mutation)
    Audit.create(
      :user_id => user.id,
      :mutation => mutation.downcase,
      :model_affected => model.downcase,
      :action => "#{message} by #{user.username}"
    )
	end
end
