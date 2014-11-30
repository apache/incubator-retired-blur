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
/*global blurconsole:false, Intl:false */
blurconsole.utils = (function(){
  'use strict';

    //-------------------------- Public API ----------------------------
  function inject(collection, initial, block) {
    if (collection === null || collection.length === 0) {
      return initial;
    }

    var accumulator = initial === null ? collection[0] : initial;
    $.each(collection, function(idx, item) {
      accumulator = block(accumulator, item);
    });

    return accumulator;
  }

  function formatNumber(num){
    if(typeof Intl !== 'undefined' && typeof Intl.NumberFormat !== 'undefined') {
      num = Intl.NumberFormat().format(num);
    }
    return num;
  }

  function unique(collection, sort) {
    var uniqueList = [];

    $.each(collection, function(idx, item){
      if (uniqueList.indexOf(item) === -1) {
        uniqueList.push(item);
      }
    });

    if (sort) {
      uniqueList.sort();
    }

    return uniqueList;
  }

  function equals(obj1, obj2) {
    return JSON.stringify(obj1) === JSON.stringify(obj2);
  }

  function keys(map) {
    return $.map(map, function(v, key){ return key; });
  }

  function findFamilies(query) {
    // Determine regex to find column families in lucene query
    var matches = query.match(/[^ \(\)\+\-]+(\w+)\.\w+:/g);

    if (matches === null) {
      return [];
    }

    var families = [];
    var family;
    $.each(matches, function(idx, match) {
      family = match.split('.')[0];
      if(families.indexOf(family) < 0) {
        families.push(family);
      }
    });
    return families;
  }

  function reject(collection, block) {
    var newArray = [];
    $.each(collection, function(i, item){
      if (!block(item)) {
        newArray.push(item);
      }
    });
    return newArray;
  }

  return {
    inject: inject,
    reduce: inject,
    unique: unique,
    equals: equals,
    keys: keys,
    findFamilies: findFamilies,
    reject: reject,
    formatNumber: formatNumber
  };
}());