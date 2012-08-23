require 'spec_helper'

describe Zookeeper do

  describe 'translated_status' do
    it 'returns Offline when the status is 0' do
      zk = Zookeeper.new :status => 0
      zk.translated_status.should == 'Offline'
    end

    it 'returns Online when the status is 1' do
      zk = Zookeeper.new :status => 1
      zk.translated_status.should == 'Online'
    end
  end

end
