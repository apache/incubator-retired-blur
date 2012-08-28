class Audit < ActiveRecord::Base
  belongs_to :user

  scope :recent, lambda { |from, to|
    where(:created_at => from.hours.ago..to.hours.ago).
    includes(:user)
  }

	def self.log_event(user, message, model, mutation, zookeeper_affected)
    Audit.create(
      :user_id => user.id,
      :mutation => mutation.downcase,
      :model_affected => model.downcase,
      :action => "#{message}",
      :zookeeper_affected => zookeeper_affected
    )
	end

  def summary
    {
      :action => action,
      :date_audited => created_at.getutc.to_s,
      :model => model_affected,
      :mutation => mutation,
      :username => user.username,
      :user => user.name,
      :zookeeper_affected => zookeeper_affected
    }
  end
end
