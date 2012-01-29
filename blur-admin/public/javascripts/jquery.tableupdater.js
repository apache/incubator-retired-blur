(function($) {
    $.widget("ui.tableupdater", {
       _previousJsonIds: [],
       _currentDelay: 0,
       options: {
           url: null,
           delay: 5000,
           decay: 1,
           maxDelay: 60000,
           noDataMessage: null,
           change: null,
           destroy: null,
           editValueBeforeCellComparison: null,
           customUpdate: null
       },

       _create: function() {
           var self = this;
           self._currentDelay = self.options.delay;
           self._updateTable();
       },

       _updateTable: function() {
           var self = this;
           var changed = false;
           $.ajax({
              url: self.options.url,
              dataType: 'json',
              success: function(data) {
                  changed = self._updateRows(data);
              },
              complete: function() {
                  if (self.options.delay) {
                      if (changed) {
                          self._currentDelay = self.options.delay;
                      } else {
                          self._currentDelay = self._currentDelay * self.options.decay;
                          if (self._currentDelay > self.options.maxDelay) {
                              self._currentDelay = self.options.maxDelay;
                          }
                      }
                      setTimeout(function(){self._updateTable()},self._currentDelay);
                  }
              }
           });
       },

       _updateRows: function(currentJson) {
           var self = this;
           var table = self.element;
           var didAnythingChange = false;
           var firstTime = !self._previousJsonIds.length;
           var currentDataIds = [];
           var previousDataIds = [];

           if (firstTime) {
               var trs = table.find('tbody tr');
               for (var i = 0;  i < trs.length; i++) {
                 $(trs[i]).remove();
               }
           }

           for (var currentIndex = 0; currentIndex < currentJson.length; currentIndex++) {
               currentDataIds[currentIndex] = currentJson[currentIndex].id;
           }

           for (var previousIndex = 0; previousIndex < self._previousJsonIds.length; previousIndex++) {
               var previousId = self._previousJsonIds[previousIndex];
               previousDataIds[previousIndex] = previousId;

               //Delete old rows
               if (currentDataIds.indexOf(previousId) == -1) {
                   var row = table.find("#" + self._getRowId(previousId));
                   if (self.options.destroy) {
                       self.options.destroy(row);
                   }
                   row.remove();
                   didAnythingChange = true;
               }
           }

           for (var currentJsonIndex = 0; currentJsonIndex < currentJson.length; currentJsonIndex++) {
               var item = currentJson[currentJsonIndex];
               if (previousDataIds.indexOf(item.id) == -1) {
                   self._createRow(item);
                   didAnythingChange = true;
               }

               //Edit existing rows
               if (self._updateRow(item)) {
                   didAnythingChange = true;
               }
           }

           if (didAnythingChange || firstTime) {
               self.options.change(table);
           }

           self._handleNoDataMessage(table);
           self._previousJsonIds = currentDataIds;
           table = null;
           return didAnythingChange;
       },

       _handleNoDataMessage: function(table) {
           var self = this;
           if (self.options.noDataMessage) {
               var trs = table.find('tbody tr');
               var noDataId = table.attr("id") + '_noDataMessage';
               if (trs.size() == 0) {
                   var headerCount = table.find("thead>tr>th").size();
                   table.find('tbody').append("<tr id='" + noDataId + "'><td colspan='" + headerCount + "'>" + self.options.noDataMessage + "</td></tr>");
                   return;
               }
               var noDataMessageTr = table.find('tbody tr#' + noDataId);
               if (noDataMessageTr.size() == trs.size()) {
                   return;
               }
               noDataMessageTr.remove();
           }
       },

       _getRowId: function(itemId) {
           var id = this.element.attr("id") + "_" + itemId;
           id = id.replace(/[^\w]/gi, '');
           return id;
       },

       _createRow: function(item) {
           var self = this;
           var table = self.element;
           var cellKeys = $.map(table.find('thead tr:first th'), function(header){
              return $(header).attr('data-key');
           });
           var row = $('<tr></tr>').attr('id', self._getRowId(item.id));
           for (var i = 0; i < cellKeys.length; i++) {
               row.append($('<td></td>').attr('data-key', cellKeys[i]));
           }
           table.find('tbody').append(row);
           table = null;
       },

       _updateRow: function(item) {
           var self = this;
           var table = self.element;
           var didAnythingChange = false;

           var row = table.find('#' + self._getRowId(item.id));
           var cells = row.children();

           for (var i = 0; i < cells.length; i++) {
               var cell = $(cells[i]);
               var key = cell.attr('data-key');
               var value = item[key];

               if (self.options.editValueBeforeCellComparison) {
                   value = self.options.editValueBeforeCellComparison(value) || value;
               }

               if (self.options.customUpdate && self.options.customUpdate[key]) {
                   if (self.options.customUpdate[key](cell, value)) {
                       didAnythingChange = true;
                   }
               } else {
                   if (value !== undefined && cell.html() != value) {
                       cell.html(value);
                       didAnythingChange = true;
                   }
               }

               cell = null;
           }

           cells = null;
           row = null;
           table = null;
           return didAnythingChange;
       }
    });
})(jQuery);