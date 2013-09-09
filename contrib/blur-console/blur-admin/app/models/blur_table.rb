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
class BlurTable < ActiveRecord::Base
  require 'blur_thrift_client'

  belongs_to :cluster
  has_many :blur_queries, :dependent => :destroy
  has_many :searches, :dependent => :destroy
  has_one :zookeeper, :through => :cluster

  scope :deleted, where("table_status=?", 0)
  scope :disabled, where("table_status=?", 2)
  scope :active, where("table_status=?", 4)

  attr_accessor :query_count

  def as_json(options={})
    serial_properties = super(options)
    serial_properties.delete('server')
    serial_properties.delete('table_schema')
    serial_properties.delete('updated_at')
    serial_properties['queried_recently'] = self.query_count > 0

    host_count = self.hosts.keys.length
    shard_count = 0
    self.hosts.values.each{ |shards| shard_count += shards.length }

    serial_properties['server_info'] = host_count.to_s + ' | ' + shard_count.to_s
    serial_properties['comments'] = self.comments
    serial_properties
  end

  # Returns a map of host => [shards] of all hosts/shards associated with the table
  def hosts
    read_attribute(:server).blank? ? {} : (JSON.parse read_attribute(:server))
  end

  def schema
    return nil if self.table_schema.blank?
    # sort columns inline
    sorted_schema = (JSON.parse self.table_schema).each{|n| n['columns'].sort_by!{|k| k['name']}}
    if block_given?
      sorted_schema.sort &Proc.new
    else
      # sort column families
      sorted_schema.sort_by{|k| k['name']}
    end
  end

  def record_count
    read_attribute(:record_count).to_s.reverse.gsub(%r{([0-9]{3}(?=([0-9])))}, "\\1#{','}").reverse
  end

  def row_count
    read_attribute(:row_count).to_s.reverse.gsub(%r{([0-9]{3}(?=([0-9])))}, "\\1#{','}").reverse
  end

  def is_enabled?
    self.table_status == 4
  end

  def is_disabled?
    self.table_status == 2
  end

  def is_deleted?
    self.table_status == 0
  end

  def terms(blur_urls,family,column,startWith,size)
    ActiveSupport::Notifications.instrument "terms.blur", :urls => blur_urls, :family => family, :column => column, :starter => startWith, :size => size do
      BlurThriftClient.client(blur_urls).terms(self.table_name, family, column, startWith, size)
    end
  end

  def enable(blur_urls)
    begin
      ActiveSupport::Notifications.instrument "enable.blur", :urls => blur_urls, :table => self.table_name do
        BlurThriftClient.client(blur_urls).enableTable self.table_name
      end
    ensure
      return self.is_enabled?
    end
  end

  def disable(blur_urls)
    begin
      ActiveSupport::Notifications.instrument "disable.blur", :urls => blur_urls, :table => self.table_name do
        BlurThriftClient.client(blur_urls).disableTable self.table_name
      end
    ensure
      return self.is_disabled?
    end
  end

  def blur_destroy(underlying=false, blur_urls)
    begin
      ActiveSupport::Notifications.instrument "destroy.blur", :urls => blur_urls, :table => self.table_name, :underlying => underlying do
        BlurThriftClient.client(blur_urls).removeTable self.table_name, underlying
      end
      return true
    rescue
      return false
    end
  end
end
