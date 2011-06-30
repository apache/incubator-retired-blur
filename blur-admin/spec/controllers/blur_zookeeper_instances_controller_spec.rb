require 'spec_helper'

describe ZookeepersController do
  before(:each) do
    @ability = Ability.new User.new
    @ability.stub!(:can?).and_return(true)
    controller.stub!(:current_ability).and_return(@ability)
  end

  describe 'GET show' do
    it "sets the @zookeepers, @controlers, @clusters, and @shards variables" do
      pending "finish testing once entity relations are finalized"
    end
  end
end
