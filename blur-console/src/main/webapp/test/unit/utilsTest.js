/*

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/
/* global describe, it, expect, blurconsole */

'use strict';

describe('Test blurconsole.utils', function () {
    describe('inject', function () {
        it('[1,2,3,4,5] should be 15 with simple summing', function () {
			expect(blurconsole.utils.inject([1,2,3,4,5], 0, function(sum, item){ return sum + item; })).to.equal(15);
        });

        it('null collection should return initial value', function() {
			expect(blurconsole.utils.inject(null, 0, function(sum, item){ return item; })).to.equal(0);
        });
    });
});