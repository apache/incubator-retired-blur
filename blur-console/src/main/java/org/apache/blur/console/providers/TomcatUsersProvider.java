package org.apache.blur.console.providers;

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

import org.apache.blur.BlurConfiguration;
import org.apache.blur.console.model.User;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

import javax.servlet.http.HttpServletRequest;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.*;

/**
 * Provider that reads from a tomcat-users.xml file
 * config blur.console.auth.provider.tomcat.usersfile to point to the xml file
 * This file gets read once at startup
 */
public class TomcatUsersProvider extends BaseProvider {

  private static class TomcatUser extends User {

    private String password;

    public TomcatUser(String name, String password, Collection<String> roles) {
      this.name = name;
      this.password = password;
      this.roles = roles;
    }

    private boolean checkPassword(String passwd) {
      return password.equals(passwd);
    }
  }

  private Map<String, TomcatUser> users = new HashMap<String, TomcatUser>();

  @Override
  public User login(HttpServletRequest request) {
    Map<String, String[]> parameters = request.getParameterMap();
    String[] usernames = parameters.get("username");
    String[] passwords = parameters.get("password");
    if (usernames != null && usernames.length > 0 && passwords != null && passwords.length > 0) {
      String username = usernames[0];
      String password = passwords[0];
      TomcatUser user = users.get(username);
      if (user != null && user.checkPassword(password)) {
        return user;
      }
    }
    return null;
  }

  @Override
  protected void setupProviderInternal(BlurConfiguration config) throws IOException, JDOMException {
    String usersFile = config.get("blur.console.auth.provider.tomcat.usersfile");
    SAXBuilder builder = new SAXBuilder();
    Reader in = new FileReader(usersFile);
    Document doc = builder.build(in);
    Element root = doc.getRootElement();
    List<Element> xmlUsers = root.getChildren("user");
    for (Element user : xmlUsers) {
      String username = user.getAttribute("username").getValue();
      String password = user.getAttribute("password").getValue();
      String roles = user.getAttribute("roles").getValue();
      Collection<String> splitRoles = Arrays.asList(roles.split(","));
      users.put(username, new TomcatUser(username, password, mapRoles(splitRoles)));
    }
  }

  @Override
  public String getLoginForm() {
    String html = "<form>" +
      "<div class=\"form-group\">" +
      "<label for=\"username\">Username</label>" +
      "<input name=\"username\" class=\"form-control\"/>" +
      "</div>" +
      "<div class=\"form-group\">" +
      "<label for=\"password\">Password</label>" +
      "<input type=\"password\" name=\"password\" class=\"form-control\"/>" +
      "</div>" +
      "<button type=\"submit\" class=\"btn btn-default\">Submit</button>" +
      "</form>";
    return html;
  }
}
