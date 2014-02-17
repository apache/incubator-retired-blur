# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with this
# work for additional information regarding copyright ownership. The ASF
# licenses this file to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
class Audit < ActiveRecord::Base
  belongs_to :user

  scope :recent, lambda { |from, to|
    where(:created_at => from.hours.ago..to.hours.ago).
    includes(:user)
  }

	def self.log_event(user, message, model, mutation, parent_affected)
    Audit.create(
      :user_id => user.id,
      :mutation => mutation.downcase,
      :model_affected => model.downcase,
      :action => "#{message}",
      :zookeeper_affected => parent_affected.name
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
