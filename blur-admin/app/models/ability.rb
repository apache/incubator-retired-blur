class Ability
  include CanCan::Ability

  def initialize(user)

    if user # logged in

      # view, edit, and destroy own account
      can [:show, :edit, :destroy], :users, :id => user.id

      # edit own username, email, password
      can :update, :users, [:username, :email, :password, :password_confirmation], :id => user.id

      # logout
      can :destroy, :user_sessions

      if user.has_role? :reader

        # view pages
        can :index, [:blur_tables, :search]
        can :show, [:zookeepers, :search]

        # search
        can [:filters, :create], :search

        # can view everything but query_string on blur_tables:
        attributes = BlurQuery.new.attribute_names
        attributes.delete "query_string"
        attributes.collect! {|attribute| attribute.to_sym}
        can :index, :blur_queries, attributes

        # view more info on blur_queries on with everything but query_string
        can :more_info, :blur_queries, attributes

        # View schema on blur_tables
        can :hosts, :blur_tables

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
        can :update, :users, [:admin, :editor, :reader, :auditor]
      end

    else  # not logged in
      can :new, [:users, :user_sessions]
      can :create, :user_sessions
      can :create, :users, [:username, :email, :password, :password_confirmation]
    end



    # Define abilities for the passed in user here. For example:
    #
    #   user ||= User.new # guest user (not logged in)
    #   if user.admin?
    #     can :manage, :all
    #   else
    #     can :read, :all
    #   end
    #
    # The first argument to `can` is the action you are giving the user permission to do.
    # If you pass :manage it will apply to every action. Other common actions here are
    # :read, :create, :update and :destroy.
    #
    # The second argument is the resource the user can perform the action on. If you pass
    # :all it will apply to every resource. Otherwise pass a Ruby class of the resource.
    #
    # The third argument is an optional hash of conditions to further filter the objects.
    # For example, here the user can only update published articles.
    #
    #   can :update, Article, :published => true
    #
    # See the wiki for details: https://github.com/ryanb/cancan/wiki/Defining-Abilities
  end
end
