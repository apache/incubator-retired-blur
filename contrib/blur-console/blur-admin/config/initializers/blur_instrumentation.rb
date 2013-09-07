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
ActiveSupport::Notifications.subscribe "search.blur" do |name, start, finish, id, payload|
  item_name = '%s (%dms)' % ["Blur Query", (finish - start)]
  Rails.logger.debug("  #{item_name}  [#{payload[:table]}] #{payload[:query]} - #{payload[:urls]}")
end

ActiveSupport::Notifications.subscribe "terms.blur" do |name, start, finish, id, payload|
  item_name = '%s (%dms)' % ["Blur Terms", (finish - start)]
  Rails.logger.debug("  #{item_name}  [#{payload[:family]}.#{payload[:column]}] #{payload[:starter]} (#{payload[:size]}) - #{payload[:urls]}")
end

ActiveSupport::Notifications.subscribe "enable.blur" do |name, start, finish, id, payload|
  item_name = '%s (%dms)' % ["Blur Enable Table", (finish - start)]
  Rails.logger.debug("  #{item_name}  [#{payload[:table]}] - #{payload[:urls]}")
end

ActiveSupport::Notifications.subscribe "disable.blur" do |name, start, finish, id, payload|
  item_name = '%s (%dms)' % ["Blur Disable Table", (finish - start)]
  Rails.logger.debug("  #{item_name}  [#{payload[:table]}] - #{payload[:urls]}")
end

ActiveSupport::Notifications.subscribe "destroy.blur" do |name, start, finish, id, payload|
  item_name = '%s (%dms)' % ["Blur Destroy Table", (finish - start)]
  Rails.logger.debug("  #{item_name}  [#{payload[:table]}] (Underlying: #{payload[:underlying]}) - #{payload[:urls]}")
end

ActiveSupport::Notifications.subscribe "cancel.blur" do |name, start, finish, id, payload|
  item_name = '%s (%dms)' % ["Blur Cancel Query", (finish - start)]
  Rails.logger.debug("  #{item_name}  [#{payload[:table]}] #{payload[:uuid]} - #{payload[:urls]}")
end