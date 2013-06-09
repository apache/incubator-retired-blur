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

FactoryGirl.define do
  factory :hdfs_stat do
    config_capacity       { 100000 + rand(10000) }
    present_capacity      { config_capacity - rand(50000) }
    dfs_used_real         { rand 50000 }
    dfs_used_logical      { rand 50000 }
    dfs_remaining         { present_capacity - dfs_used_real }
    dfs_used_percent      { (present_capacity - dfs_used_real) / present_capacity }
    under_replicated      { rand 5 }
    corrupt_blocks        { rand 5 }
    missing_blocks        { rand 5 }
    total_nodes           { rand 5 }
    dead_nodes            { rand 5 }
    sequence(:created_at, 0) {|n| ((n % 3) + 0.9).minute.ago }
  end
end
