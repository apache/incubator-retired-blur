require "spec_helper"

describe EnvController do
  render_views
  before(:each) do
    @ability = Ability.new User.new
    @ability.stub!(:can?).and_return(true)
    controller.stub!(:current_ability).and_return(@ability)
    controller.stub!(:current_user).and_return(@user)
  end

  it "displays a widget for HDFS status, Zookeeper Status, and Blur Status" do
    get :show
    response.should render_template :show
    response.body.should include("HDFS Status")
    response.body.should include("Zookeeper Status")
    response.body.should include("Blur Status")
  end
end
