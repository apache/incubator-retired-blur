class Audit < ActiveRecord::Base
  belongs_to :user

  scope :recent, lambda { |time|
    where(:created_at => time.hours.ago..Time.now).
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
