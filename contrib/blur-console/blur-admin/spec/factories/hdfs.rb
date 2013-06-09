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
  factory :hdfs do
    host { "nic-factory-hdfs.com" }
    port { "9000" }
    name { "factory_hdfs" }

    ignore do
      recursive_factor 3
    end

    factory :hdfs_with_stats do
      after_create do |hdfs, evaluator|
        FactoryGirl.create_list(:hdfs_stat, evaluator.recursive_factor, :hdfs => hdfs)
      end
    end
  end
end