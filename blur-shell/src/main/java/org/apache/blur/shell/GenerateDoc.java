/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.blur.shell;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Map;

public class GenerateDoc {

  public static void main(String[] args) throws IOException {
    Main.setupCommands();
    Map<String, Command> commands = Main.commands;
    ByteArrayOutputStream outputStream1 = new ByteArrayOutputStream();
    ByteArrayOutputStream outputStream2 = new ByteArrayOutputStream();
    PrintWriter menu = new PrintWriter(outputStream1);
    PrintWriter body = new PrintWriter(outputStream2);
    printCommands("Table Commands", Main.tableCommands, commands, menu, body);
    printCommands("Data Commands", Main.dataCommands, commands, menu, body);
    printCommands("Cluster Commands", Main.clusterCommands, commands, menu, body);
    printCommands("Server Commands", Main.serverCommands, commands, menu, body);
    printCommands("Platform Commands", Main.platformCommands, commands, menu, body);
    printCommands("Shell Commands", Main.shellCommands, commands, menu, body);
    menu.close();
    body.close();
    String menuReplacementText = new String(outputStream1.toByteArray());
    String bodyReplacementText = new String(outputStream2.toByteArray());

    File source = new File(args[0]);
    File output = new File(args[1]);

    PrintWriter writer = new PrintWriter(output);

    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(source)));
    String line;
    while ((line = reader.readLine()) != null) {
      if (line.equals("|||Shell-Menu|||")) {
        writer.println(menuReplacementText);
      } else if (line.equals("|||Shell-Body|||")) {
        writer.println(bodyReplacementText);
      } else {
        writer.println(line);
      }
    }
    writer.close();
    reader.close();
  }

  private static void printCommands(String name, String[] commandNames, Map<String, Command> commands,
      PrintWriter menu, PrintWriter body) {
    String cleanedName = name.toLowerCase().replace(" ", "_");
    menu.println("<li><a href=\"#shell_" + cleanedName + "\">" + name + "</a>");
    menu.println("<ul class=\"nav\">");
    body.println("<h3 id=\"shell_" + cleanedName + "\">" + name + "</h3>");
    for (String commandName : commandNames) {
      Command command = commands.get(commandName);
      String description = command.description();
      String usage = escapeForHtml(command.usage());
      menu.println("<li><a href=\"#shell_command_" + commandName + "\">&nbsp;&nbsp;" + commandName + "</a></li>");
      body.println("<h4 id=\"shell_command_" + commandName + "\">" + commandName + "</h4>");
      body.println("<p>Description: " + description + "<br/>");
      body.println("<pre><code class=\"bash\">" + commandName + " " + usage + "</code></pre></p>");
    }
    menu.println("</ul></li>");
  }

  private static String escapeForHtml(String text) {
    return text.replace("<", "&lt;").replace(">", "&gt;");
  }
}
