require 'test_helper'

class ConfigControllerTest < ActionController::TestCase
  test "should get index" do
    get :index
    assert_response :success
  end

end
