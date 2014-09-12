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

+function ($) {
  'use strict';

  // ONELINETABS CLASS DEFINITION
  // ======================
  var OneLineTabs = function (element, options) {
    $(window)
      .on('resize.bs.tabs.oneline', $.proxy(this.resize, this))
    this.$element = $(element)
    this.resize();
  }

  OneLineTabs.prototype.resize = function () {
    var dropdown = this.$element.find('>li.dropdown');
    if(dropdown.length > 0) {
      this.$element.append(dropdown.find('li'));
      dropdown.remove();
    }
    var lis = this.$element.find('>li:not(.dropdown)');
    if(lis.length > 0) {
      var first_top = $(lis[0]).position().top;
      var last_top = $(lis[lis.length-1]).position().top;
      if(first_top === last_top) {
        return; // no collapsing needed
      }
      // add dropdown
      this.$element.append('<li class="dropdown"><a id="myTabDrop1" data-toggle="dropdown" class="dropdown-toggle" href="#">More <span class="caret"></span></a><ul class="dropdown-menu" role="menu" aria-labelledby="myTabDrop1"></ul></li>');
      dropdown = this.$element.find('>li.dropdown');
      var dropdown_ul = dropdown.find('ul');
      var i = 1; // skip first one
      while(i < lis.length && $(lis[i]).position().top === first_top) {
        i++;
      }
      i--;
      if($(lis[i]).width() < dropdown.width()) {
        i--;
      }
      while(i < lis.length) {
        dropdown_ul.append(lis[i]);
        i++;
      }
    }
  }

  // ONELINETABS PLUGIN DEFINITION
  // =======================
  function Plugin(option) {
    return this.each(function () {
      var $this   = $(this)
      var data    = $this.data('bs.onelinetabs')
      var options = typeof option == 'object' && option

      if (!data) $this.data('bs.onelinetabs', (data = new OneLineTabs(this, options)))
      if (typeof option == 'string') data[option]()
    })
  }

  var old = $.fn.onelinetabs

  $.fn.onelinetabs             = Plugin
  $.fn.onelinetabs.Constructor = OneLineTabs

  // ONELINETABS NO CONFLICT
  // =================
  $.fn.onelinetabs.noConflict = function () {
    $.fn.onelinetabs = old
    return this
  }

}(jQuery);
