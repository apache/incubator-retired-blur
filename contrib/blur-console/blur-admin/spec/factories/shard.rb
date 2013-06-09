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
  factory :blur_shard do
    blur_version              { "1.#{rand 10}.#{rand 10}" }
    sequence (:node_name)     { |n| "Test Node ##{n}" }
    shard_status                    { rand 3 }
    
    factory :shard_with_cluster do
      after_create do |blur_shard|
        FactoryGirl.create_list(:cluster, 1, :blur_shards => [blur_shard])
      end
    end
  end
end