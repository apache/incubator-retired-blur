require 'spec_helper'

describe ZookeepersController do
  before(:each) do
    @ability = Ability.new User.new
    @ability.stub!(:can?).and_return(true)
    controller.stub!(:current_ability).and_return(@ability)

  end

  describe 'GET show_current' do
    it "sets the @zookeeper variable" do
      pending "finish testing once entity relations are finalized"
    end
  end
end
