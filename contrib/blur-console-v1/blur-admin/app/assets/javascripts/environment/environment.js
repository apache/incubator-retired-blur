/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//= require bootstrap-tooltip
//= require bootstrap-popover
//= require_tree .

// Confirmation popup for forgetting parts of the ZK
var Confirm_Delete = function(options){
  $().popup({
      title: "Are you sure?",
      titleClass: 'title',
      body: '<div>Are you sure that you want to ' + options.message + '?</div>',
      btns: {
        "Remove": {
          "class": "danger",
          func: function(){
            options.confirmed_action();
            $().closePopup();
          }
        },
        "Cancel": {
          func: function() {
            $().closePopup();
          }
        }
      }
    });
};

$(document).ready(function(){
  // Start streaming the model on 5 sec intervals
  new ZookeeperModel().stream(5000);
});
