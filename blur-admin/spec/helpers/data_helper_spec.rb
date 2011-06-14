require "spec_helper"

describe DataHelper do
  before do
    @ability = Ability.new User.new
    @ability.stub!(:can?).and_return(true)
    controller.stub!(:current_ability).and_return(@ability)
    
  end
 
  describe "#shards" do
    it "returns the shards and hosts" do
      table_server = {'a_table' => {'a_shard' => 'a_host'}}
      assign(:tserver, table_server)
      returned_host = {'a_host'=>['a_shard']}
      helper.shards('a_table').should eq(returned_host)
    end
  end
end

