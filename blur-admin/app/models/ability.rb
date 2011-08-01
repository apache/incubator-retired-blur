class Ability
  include CanCan::Ability

  def initialize(user)
    can :update, :preferences

    if user # logged in

      # view, edit, and destroy own account
      can [:show, :edit, :destroy], :users, :id => user.id

      # edit own username, email, password
      can :update, :users, [:username, :email, :password, :password_confirmation], :id => user.id

      # logout
      can :destroy, :user_sessions

      if user.has_role? :reader

        # view pages
        can :index, [:zookeepers, :blur_tables, :hdfs]
        can :show, :zookeepers
        can :show_current, :zookeepers
        can :make_current, :zookeepers
        can :dashboard, :zookeepers
        can :files, :hdfs
        can :jstree, :hdfs



        # can view everything but query_string on blur_tables:
        attributes = BlurQuery.new.attribute_names
        attributes.delete "query_string"
        attributes.collect! {|attribute| attribute.to_sym}
        can :index, :blur_queries, attributes

        # view more info o[M `Dr_queries on with everything but query_string
        can :more_info, :blur_queries, attributes
        can :refresh, :blur_queries

        # View hosts and schema on blur_tables
        can :hosts, :blur_tables
        can :schema, :blur_tables

      end

      if user.has_role? :editor
        can [:update, :destroy], :blur_tables
        can :update, :blur_queries
      end

      if user.has_role? :auditor
        can :index, :blur_queries, :query_string
        can :more_info, :blur_queries, :query_string
      end

      if user.has_role? :admin
        can [:show, :index, :edit, :destroy, :create, :new], :users
        can :update, :users, User.valid_roles
      end

      if user.has_role? :searcher
        # search
        can [:show, :filters, :create, :load, :delete, :reload, :save, :update], :search
      end

    else  # not logged in
      can :new, [:users, :user_sessions]
      can :create, :user_sessions
      can :create, :users, [:username, :email, :password, :password_confirmation]
    end
  end
end
