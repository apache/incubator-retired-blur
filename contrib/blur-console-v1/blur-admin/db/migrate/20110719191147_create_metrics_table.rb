#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#  
#  http://www.apache.org/licenses/LICENSE-2.0
#  
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

class CreateMetricsTable < ActiveRecord::Migration
  def self.up
    create_table :system_metrics, :force => true do |t|
      t.column :name, :string, :null => false
      t.column :started_at, :datetime, :null => false
      t.column :transaction_id, :string
      t.column :payload, :text
      t.column :duration, :float, :null => false
      t.column :exclusive_duration, :float, :null => false
      t.column :request_id, :integer
      t.column :parent_id, :integer
      t.column :action, :string, :null => false
      t.column :category, :string, :null => false
    end

    # TODO: Add indexes
  end

  def self.down
    drop_table :system_metrics
  end
end
