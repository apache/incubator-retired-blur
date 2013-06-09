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
class ErrorsController < ApplicationController
  respond_to :json, :html

  def error_404
    render :error_404, :status => :not_found
  end

  def error_500
    @error = env["action_dispatch.exception"]
    @status_code = ActionDispatch::ExceptionWrapper.new(env, @exception).status_code
    render :error_500, :status => 500
  end

  def error_422
    render :error_422, :status => 422
  end
end
