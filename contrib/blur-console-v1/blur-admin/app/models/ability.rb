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
class Ability
  include CanCan::Ability

  def initialize(user)
    #Anybody
    can [:error_404, :error_422, :error_500], :errors

    if user # logged in
      # view, edit, and destroy own account
      can [:show, :edit, :destroy], :users, :id => user.id

      # edit own username, email, password
      can :update, :users, [:username, :name, :email, :password, :password_confirmation], :id => user.id

      # logout
      can :destroy, :user_sessions

      if user.reader?
        # view pages
        can :index, [:zookeepers, :blur_tables, :hdfs, :hdfs_metrics]
        can :show, [:zookeepers, :clusters]
        can :long_running_queries, :zookeepers
        can [:expand, :file_info, :info, :folder_info, :slow_folder_info, :file_tree], :hdfs
        can :stats, :hdfs_metrics
        can :help, :application

        # can view everything but query_string on blur_tables:
        attributes = BlurQuery.new.attribute_names.collect{|att| att.to_sym}
        attributes.delete :query_string
        can [:index, :show], :blur_queries, attributes

        can :refresh, :blur_queries
        can [:terms, :hosts, :schema], :blur_tables
      end

      if user.editor?
        can [:enable, :disable, :destroy, :comment], :blur_tables
        can :cancel, :blur_queries
        can :index, :blur_shards
        can [:destroy], [:zookeepers, :clusters, :blur_shards, :blur_controllers]
        can [:move_file, :delete_file, :mkdir, :upload_form, :upload], :hdfs
      end

      if user.auditor?
        can [:index, :show], :blur_queries, :query_string
        can :index, :audits
      end

      if user.admin?
        can [:index, :edit, :destroy, :create, :new], :users
        can :update, :users, [:email, :roles]
        can :update, :admin_settings
      end

      if user.searcher?
        # searches
        can :access, :searches

        # Can modify own column preferences
        can :update, :preferences, :user_id => user.id
      end

    else  # not logged in
      can :new, [:users, :user_sessions]
      can :create, :user_sessions
      can :create, :users, [:username, :name, :email, :password, :password_confirmation]
    end
  end
end
