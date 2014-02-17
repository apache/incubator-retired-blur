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
  factory :cluster do
    sequence(:name)  { |n| "Test Cluster ##{n}" }
    safe_mode        false
    zookeeper_id     1
    can_update       false

    ignore do
      recursive_factor 3
    end

    factory :cluster_with_shard do
      after_create do |cluster|
        FactoryGirl.create_list(:blur_shard, 1, :cluster => cluster)
      end
    end

    factory :cluster_with_shards do
      after_create do |cluster|
        FactoryGirl.create_list(:blur_shard, 3, :cluster => cluster)
      end
    end

    factory :cluster_with_shards_online do
      after_create do |cluster|
        FactoryGirl.create_list(:blur_shard, 3, :cluster => cluster, :shard_status => 1)
      end
    end
  end
end
