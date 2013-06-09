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
  factory :blur_query do
    sequence(:query_string) {|n| "Blur Query ##{n} Query String"}
    complete_shards         { rand 6 }
    uuid                    { rand 10 ** 8 }
    super_query_on          { true } # 75% chance
    start                   { rand 10 ** 6 }
    fetch_num               { rand 10 ** 6 }
    userid                  { "Test User ##{rand 20}" }
    times                   {{'shard' => { :cpuTime => 40, :realTime => 56, :setCpuTime => true, :setRealTime => true }}.to_json}
    total_shards            { 5 }
    state                   { rand 3 }
  end
end