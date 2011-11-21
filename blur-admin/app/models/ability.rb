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

      if user.has_role? :reader

        # view pages
        can :index, [:zookeepers, :blur_tables, :hdfs]
        can :show, [:zookeepers, :help]
        can [:show_current, :make_current], :zookeepers
        can :dashboard, :zookeepers
        can [:expand, :file_info, :info], :hdfs
        can :help, :application

        # can view everything but query_string on blur_tables:
        attributes = BlurQuery.new.attribute_names.collect{|att| att.to_sym}
        attributes.delete :query_string
        can :index, :blur_queries, attributes
        can :long_running, :blur_queries, attributes

        # view more info on queries with everything but query_string
        can :more_info, :blur_queries, attributes
        can :refresh, :blur_queries

        # view times on blur queries
        can :times, :blur_queries

        # Can modify own filter preferences
        can :update, :preferences, {:user_id => user.id, :pref_type => 'filter'}

        # View hosts and schema on blur_tables
        can :hosts, :blur_tables
        can :schema, :blur_tables
        can :reload, :blur_tables

      end

      if user.has_role? :editor
        can [:update, :destroy, :update_all, :delete_all, :forget, :forget_all], :blur_tables
        can :update, :blur_queries
        can [:destroy_shard, :destroy_controller], :zookeepers
        can [:move_file, :delete_file, :mkdir], :hdfs
      end

      if user.has_role? :auditor
        can :index, :blur_queries, :query_string
        can :more_info, :blur_queries, :query_string
      end

      if user.has_role? :admin
        can [:index, :edit, :destroy, :create, :new], :users
        can :update, :users, [:email,User.valid_roles]
      end

      if user.has_role? :searcher
        # search
        can :access, :search

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
