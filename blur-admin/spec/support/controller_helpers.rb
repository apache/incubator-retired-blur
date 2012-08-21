# Setup method for the controllers
module ControllerHelpers
  # Setup the universal variables and stubs
  def setup_variables_and_stubs
    # Create a user for the abilility filter
    @user = FactoryGirl.create :user_with_preferences
    @ability = Ability.new @user

    # Allow the user to perform all of the actions
    @ability.stub!(:can?).and_return(true)

    # Stub out auditing in the controllers
    Audit.stub!(:log_event)
  end

  # Stub out the current ability in the application controller
  def set_ability
    controller.stub!(:current_ability).and_return(@ability)
  end

  # Stub out the current user in the application controller
  def set_current_user
    controller.stub!(:current_user).and_return(@user)
  end

  # General setup (for most controllers)
  def setup_tests
    setup_variables_and_stubs
    set_ability
    set_current_user
  end
end

RSpec.configure do |config|
  config.include ControllerHelpers, :type => :controller
end