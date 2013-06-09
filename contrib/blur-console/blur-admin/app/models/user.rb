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
class User < ActiveRecord::Base
  attr_accessible :email, :username, :name, :password, :password_confirmation, :roles

  has_many :searches
  has_many :preferences
  has_many :audits

  email_regex = /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+/

  acts_as_authentic do |c|
    c.merge_validates_format_of_email_field_options({:with => email_regex} )
  end

  # declare the valid roles -- do not change the order if you add more
  # roles later, always append them at the end!
  ROLES = %w[editor admin reader auditor searcher]

  def ability
    @ability ||= Ability.new(self)
  end

  delegate :can?, :cannot?, :to => :ability

  #returns the array of saved cols
  def column_preference
    Preference.find_or_create_by_user_id_and_pref_type(self.id, 'column') do |preference|
      preference.name = 'column'
      preference.value = []
    end
  end
  #returns the zookeeper preference set by the user
  def zookeeper_preference
    Preference.find_or_create_by_user_id_and_pref_type(self.id, 'zookeeper') do |preference|
      preference.name = '0'
      preference.value = nil
    end
  end

  ### ROLE AUTHENTICATION ###

  def roles=(roles)
    self.roles_mask = (roles & ROLES).map { |r| 2**ROLES.index(r) }.sum
  end

  def roles
    ROLES.reject do |r|
      ((roles_mask || 0) & 2**ROLES.index(r)).zero?
    end
  end

  def is?(role)
    roles.include?(role.to_s)
  end

  # the roles are virtual attributes needed to use form helpers
  ROLES.each do |role|
    #truthy style methods role?
    define_method role + '?' do
      return roles.include?(role)
    end
    #form helper methods (same as above)
    define_method role do
      return roles.include?(role)
    end
  end
end
