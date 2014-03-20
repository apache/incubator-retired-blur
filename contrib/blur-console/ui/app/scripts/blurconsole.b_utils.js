/*

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
/*global blurconsole:false */
blurconsole.browserUtils = (function(){
	'use strict';
	var table;

	table = function(def, data) {
		var tableMarkup;

		tableMarkup = '<table class="table table-bordered table-condensed table-hover table-striped"><thead><tr>';

		// Add headers
		$.each(def, function(idx, colDef){
			tableMarkup += '<th>' + colDef.label + '</th>';
		});

		tableMarkup += '</tr></thead><tbody>';

		// Add content
		if (data && data.length > 0) {
			$.each(data, function(ir, row){
				tableMarkup += '<tr>';
				$.each(def, function(ic, col) {
					tableMarkup += '<td>';
					if ($.isFunction(col.key)) {
						tableMarkup += col.key(row);
					} else {
						tableMarkup += row[col.key];
					}
					tableMarkup += '</td>';
				});
				tableMarkup += '</tr>';
			});
		} else {
			tableMarkup += '<tr><td colspan="' + def.length + '">There are no items here</td></tr>';
		}

		tableMarkup += '</tbody></table>';
		return tableMarkup;
	};

	return {
		table: table
	};
}());