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
  class Cluster < ActiveRecord::Base
  belongs_to :zookeeper
  has_many :blur_shards, :dependent => :destroy
  has_many :blur_tables, :dependent => :destroy, :order => 'table_name'
  has_many :blur_queries, :through => :blur_tables, :dependent => :destroy

  attr_accessor :can_update

  def as_json(options={})
    serial_properties = super(options)

    if options[:blur_tables]
      serial_properties["cluster_queried"] = self.query_status
      serial_properties["can_update"] = self.can_update
    else
      serial_properties["shard_blur_version"] = self.shard_version
      serial_properties["shard_status"] = self.shard_status
    end

    serial_properties
  end

  def shard_version
    versions = self.blur_shards.collect{ |shard| shard.blur_version }.uniq
    if versions.length < 1
      "No shards in this Cluster!"
    else
      versions.length == 1 ? versions.first : "Inconsistent Blur Versions"
    end
  end

  def shard_status
    shard_total = self.blur_shards.length
    shards_online = self.blur_shards.select{ |shard| shard.shard_status == 1 }.length
    "#{shards_online} | #{shard_total}"
  end

  def query_status
    self.blur_tables.select{ |table| table.query_count > 0 }.length > 0
  end
end
