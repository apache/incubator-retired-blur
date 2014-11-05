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
blurconsole.auth = (function() {
  'use strict';

  var username, roles, securityNames, fakeIt, container, loginDiv;

  //------------------- Private methods --------------------------

  function _attemptLogin(evt) {
    if(evt) {
      evt.preventDefault();
    }
    var serializedForm = _getLoginDiv().find('form').serialize();
    _getLoginDiv().html('Attempting to login <img src="img/ajax-loader.gif"/>');
    $.ajax('service/auth/login', {
      type: 'POST',
      data: serializedForm,
      success: function(data) {
        if(data.loggedIn) {
          if(data.user) {
            username = data.user.name;
            roles = data.user.roles;
            securityNames = data.user.securityNames;
          }
          container.trigger('userLoggedIn');
        } else {
          if(data.formHtml) {
            _getLoginDiv().html(data.formHtml);
            _getLoginDiv().find('form').on('submit',_attemptLogin);
          } else {
            _getLoginDiv().html('Login Failed');
          }
        }
      }
    });
    return false;
  }


  function _getLoginDiv() {
    if(loginDiv) {
      return loginDiv;
    }
    loginDiv = container.find('.auth_container');
    if(loginDiv.length === 0) {
      loginDiv = $('<div class="auth_container jumbotron"></div>').appendTo(container);
    }

    return loginDiv;
  }

  //------------------- Public API -------------------------------
  function initModule($container) {
    container = $container;
    if(window.location.href.indexOf('fakeIt=') > -1) {
      fakeIt = true;
      username = 'fake user';
      container.trigger('userLoggedIn');
    } else {
      _attemptLogin();
    }
  }

  function getUsername() {
    return username;
  }

  function hasRole(role) {
    if(fakeIt === true) {
      return true;
    }
    if(roles !== null) {
      if (roles.indexOf('admin') >= 0) {
        return true;
      }
      if('manager' === role && roles.indexOf('manager') >= 0) {
        return true;
      }
      if('searcher' === role && (roles.indexOf('manager') >= 0 || roles.indexOf('searcher') >= 0)) {
        return true;
      }
    }
    return false;
  }

  function getRoles() {
    return roles;
  }

  function getSecurityNames() {
    return securityNames;
  }

  return {
    initModule: initModule,
    getUsername: getUsername,
    hasRole: hasRole,
    getRoles: getRoles,
    getSecurityNames: getSecurityNames
  };
}());