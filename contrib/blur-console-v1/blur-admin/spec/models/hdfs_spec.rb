# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with this
# work for additional information regarding copyright ownership. The ASF
# licenses this file to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

require 'spec_helper'

describe Hdfs do
  before :each do
    @hdfs = FactoryGirl.create :hdfs
    @hdfs_stat = FactoryGirl.create :hdfs_stat
    @hdfs_stat2 = FactoryGirl.create :hdfs_stat
    @hdfs.stub!(:hdfs_stats).and_return [@hdfs_stat2, @hdfs_stat]
  end

  it 'returns the most recent stats' do
    @hdfs.most_recent_stats.should == @hdfs_stat
  end

  it 'returns true if stats were updated less than 1 minute ago' do
    @hdfs_stat.created_at = Time.now
    @hdfs.recent_stats.should == true
  end

  it 'returns false if stats were not updated in the last minute' do
    @hdfs_stat.created_at = Time.now - 1000*2
    @hdfs.recent_stats.should == false
  end
end
