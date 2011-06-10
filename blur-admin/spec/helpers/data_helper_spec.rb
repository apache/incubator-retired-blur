require "spec_helper"

describe DataHelper do
  describe "#shards" do
    it "returns the shards and hosts" do
      table_server = {'a_table' => {'a_shard' => 'a_host'}}
      assign(:tserver, table_server)
      returned_host = {'a_host'=>['a_shard']}
      helper.shards('a_table').should eq(returned_host)
    end
  end
end

