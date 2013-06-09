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

require 'spec_helper'

describe AuditsController do
  describe "actions" do
    before do
      # Universal setup
      setup_tests
    end

    describe 'GET index' do
      it "renders the index template" do
        get :index
        response.should render_template 'index'
      end

      it "grabs all audits within the last two days without given hours" do
        Audit.should_receive(:recent).with(48, 0)
        get :index
      end

      it "grabs all audits within the last hours given the from hours" do
        Audit.should_receive(:recent).with(40, 0)
        get :index, :from => 40
      end

      it "grabs all audits within the last hours given the to hours" do
        Audit.should_receive(:recent).with(48, 10)
        get :index, :to => 10
      end

      it "grabs all audits within the the given range of hours" do
        Audit.should_receive(:recent).with(50, 20)
        get :index, :from => 50, :to => 20
      end
    end
  end
end
