class Ability
  include CanCan::Ability

  def initialize(user)

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
        can :show, [:zookeepers, :help]
        can [:dashboard, :long_running_queries], :zookeepers
        can [:expand, :file_info, :info, :folder_info, :slow_folder_info, :file_tree], :hdfs
        can [:stats, :most_recent_stat], :hdfs_metrics
        can :help, :application

        # can view everything but query_string on blur_tables:
        attributes = BlurQuery.new.attribute_names.collect{|att| att.to_sym}
        attributes.delete :query_string
        can :index, :blur_queries, attributes

        # view more info on queries with everything but query_string
        can :more_info, :blur_queries, attributes
        can :refresh, :blur_queries

        # view times on blur queries
        can :times, :blur_queries

        # View hosts and schema on blur_tables
        can [:terms], :blur_tables

      end

      if user.editor?
        can [:update, :enable, :disable, :destroy, :forget, :comment], :blur_tables
        can :update, :blur_queries
        can [:destroy_shard, :destroy_controller, :destroy_cluster, :destroy, :shards], :zookeepers
        can [:move_file, :delete_file, :mkdir,:upload_form,:upload], :hdfs
      end

      if user.auditor?
        can :index, :blur_queries, :query_string
        can :index, :audits
        can :more_info, :blur_queries, :query_string
      end

      if user.admin?
        can [:index, :edit, :destroy, :create, :new], :users
        can :update, :users, [:email, :roles]
      end

      if user.searcher?
        # searches
        can :access, :searches

        # Can modify own column preferences
        can :update, :preferences, {:user_id => user.id, :pref_type => 'column'}
      end

    else  # not logged in
      can :new, [:users, :user_sessions]
      can :create, :user_sessions
      can :create, :users, [:username, :name, :email, :password, :password_confirmation]
    end
  end
end
