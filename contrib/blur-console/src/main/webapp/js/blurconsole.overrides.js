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

/*
 * blurconsole.overrides.js
 * File to override globals
 */
/*global console:true, blurconsole:false */

if (typeof console === 'undefined') {
    console = (function() {
        'use strict';

        function log() {
            if(typeof blurconsole !== 'undefined' && typeof blurconsole.model !== 'undefined' && typeof blurconsole.model.logs !== 'undefined') {
                var args = Array.prototype.slice.call(arguments);
                blurconsole.model.logs.logError(args.join(' '), 'javascript');
            }
        }

        function info() {
            return console.log.apply(null, arguments);
        }

        function warn() {
            return console.log.apply(null, arguments);
        }

        function error() {
            return console.log.apply(null, arguments);
        }

        function debug() {
            return console.log.apply(null, arguments);
        }

        return {
            log: log,
            info: info,
            warn: warn,
            error: error,
            debug: debug
        };
    }());
}