module RequestHelpers
  def setup_variables
    @user = FactoryGirl.create :user_with_preferences
    @ability = Ability.new @user
    @zookeeper = FactoryGirl.create :zookeeper
    @cluster = FactoryGirl.create  :cluster_with_shards_online
    @hdfs = FactoryGirl.create :hdfs_with_stats
    @table = FactoryGirl.create :blur_table_with_blur_queries
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

  def set_current_zookeeper
    controller.stub!(:current_zookeeper).and_return(@zookeeper)
  end
  
  def login
    visit login_path
    fill_in 'user_session_username', :with => @user.username
    fill_in 'user_session_password', :with => @user.password
    click_button 'Log In'
  end

  def wait_for_ajax(timeout = Capybara.default_wait_time)
    page.wait_until(timeout) do
      page.evaluate_script('jQuery.active == 0')
    end
  end

  def wait_for_dom(timeout = Capybara.default_wait_time)
    uuid = SecureRandom.uuid
    page.find("body")
    page.evaluate_script <<-EOS
      _.defer(function() {
        $('body').append("<div id='#{uuid}'></div>");
      });
    EOS
    page.find("##{uuid}")
  end
  
  def setup_tests
    setup_variables
    set_ability
    set_current_user
    set_current_zookeeper
    login
  end
end



RSpec.configure do |config|
  config.include RequestHelpers, :type => :request
end

#These are a set of hacks to make Capybara run cleanly with Rspec testing. Taken from http://www.emmanueloga.com/2011/07/26/taming-a-capybara.html
Capybara.default_wait_time = 50
Capybara.javascript_driver = :webkit
RSpec.configure do |config|
  config.use_transactional_fixtures = true
  config.before :each do
    if Capybara.current_driver != :rack_test
      Capybara.app_host = nil
    else
      Capybara.app_host = "http://127.0.0.1"
    end
  end
end

Thread.main[:activerecord_connection] = ActiveRecord::Base.retrieve_connection

def (ActiveRecord::Base).connection
  Thread.main[:activerecord_connection]
end


#Sometimes Capybara doesn't wait for the AJAX calls to finish so this forces a pause to allow page to load
class Capybara::Driver::Webkit::Browser
  alias original_command command

  def command(name, *args)
    result = original_command(name, *args)
    sleep(1) if args.first == "click"
    result
  end
end