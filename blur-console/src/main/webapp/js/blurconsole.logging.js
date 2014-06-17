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

/*global blurconsole:false */
blurconsole.logging = (function () {
	'use strict';

	//------------------------ Configuration and State -----------------------
	var configMap = {
		emptyLogMsg: 'No Errors! Yay!',
		mainHtml: '<div class="log_display"></div>',
		buttons: [
			{ 'classes': 'btn-default', 'id': 'clear-log-button', 'label': 'Clear Logs' },
			{ 'classes': 'btn-primary', 'id': 'close-logs', 'label': 'Close', 'data': {'dismiss':'modal'} }
		]
	},
	jqueryMap = {
		modal: null
	};

	//------------------ Event Handling and DOM Methods -------------
	function _showLogging() {
		if (jqueryMap.modal === null) {
			jqueryMap.modal = $(blurconsole.browserUtils.modal('error_log_modal', 'Error Logs', configMap.mainHtml, configMap.buttons, 'large'));
			jqueryMap.modal.modal()
			.on('shown.bs.modal', function(){
				jqueryMap.logHolder = $('.log_display', jqueryMap.modal);
				_drawLogs();
			})
			.on('click', '#clear-log-button', _clearLogging);
		} else {
			jqueryMap.modal.modal('show');
		}
	}

	function _clearLogging() {
		jqueryMap.logHolder.html(configMap.emptyLogMsg);
		blurconsole.model.logs.clearErrors();
	}

	function _drawLogs() {
		var errors = blurconsole.model.logs.getLogs();

		if (jqueryMap.logHolder) {
			if (errors.length === 0) {
				jqueryMap.logHolder.html(configMap.emptyLogMsg);
			} else {
				var errorList = '<ul>';
				errors.sort(function(a, b) {
					return a.timestamp.getTime() > b.timestamp.getTime();
				});
				$.each(errors, function(i, error){
					errorList += '<li><strong>' + error.error + ' (' + error.module + ')</strong><div class="pull-right"><em>' + error.timestamp.toTimeString() + '</em></div></li>';
				});
				errorList += '</ul>';
				jqueryMap.logHolder.html(errorList);
			}
		}
	}

	//------------------ Public API ---------------------------------
	function initModule() {
		$.gevent.subscribe($(document), 'show-logging', _showLogging);
		$.gevent.subscribe($(document), 'logging-updated', _drawLogs);
	}

	return {
		initModule : initModule
	};
}());