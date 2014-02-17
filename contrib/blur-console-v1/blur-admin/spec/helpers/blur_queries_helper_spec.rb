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
describe BlurQueryHelper do
  describe "format title" do
    it "returns the nothing when it is less than 20 chars" do
      format_title("Simple").should == ''
    end

    it "returns a string with the ' +' with a line break" do
      format_title("Not Very Simple +And Kind Of Long").should == "Not Very Simple<br />+And Kind Of Long"
    end
  end

  describe "print value" do
    it "returns the default message when given conditional evaluates to false" do 
      print_value(false).should == 'Not Available'
    end

    it "returns the conditional when no block is given" do 
      print_value('hello').should == 'hello'
    end

    it "executes the block when the conditional is false" do 
      print_value(true){'hello'}.should == 'hello'
    end
  end
end
