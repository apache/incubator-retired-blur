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
package org.apache.blur.doc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CreateBlurApiHtmlPage {

  private static final String ENUM = "Enum_";
  private static final String STRUCT = "Struct_";
  private static final String SERVICE = "Service";
  private static final String FN_BLUR = "Fn_Blur_";
  private static final String STRUCTS = "Data structures";
  private static final String ENUMERATIONS = "Enumerations";
  private static final String ID = "id=\"";
  private static final String DIV_CLASS_DEFINITION = "<div class=\"definition\">";

  public static void main(String[] args) throws Exception {

    File file = new File(args[0]);
    FileInputStream inputStream = new FileInputStream(file);
    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
    String line;

    Map<String, List<String>> markers = new HashMap<String, List<String>>();

    while ((line = bufferedReader.readLine()) != null) {
      line = line.trim();
      if (line.contains(DIV_CLASS_DEFINITION)) {
        int indexOf = line.indexOf(DIV_CLASS_DEFINITION);
        int start = line.indexOf(ID, indexOf + DIV_CLASS_DEFINITION.length()) + ID.length();
        int end = line.indexOf("\"", start);
        String id = line.substring(start, end);
        if (id.startsWith(ENUM)) {
          String name = id.substring(5);
          List<String> list = markers.get(ENUMERATIONS);
          if (list == null) {
            list = new ArrayList<String>();
            markers.put(ENUMERATIONS, list);
          }
          list.add("<li><a href=\"#" + id + "\">" + name + "</a></li>");
        } else if (id.startsWith(STRUCT)) {
          String name = id.substring(7);
          List<String> list = markers.get(STRUCTS);
          if (list == null) {
            list = new ArrayList<String>();
            markers.put(STRUCTS, list);
          }
          list.add("<li><a href=\"#" + id + "\">" + name + "</a></li>");
        } else if (id.startsWith(FN_BLUR)) {
          String name = id.substring(8);
          List<String> list = markers.get(SERVICE);
          if (name.equals("query")) {
            list.add("</ul></li>");
            list.add("<li><a href=\"#" + id + "\">Data Methods</a><ul class=\"nav\">");
          } else if (name.equals("shardClusterList")) {
            list.add("</ul></li>");
            list.add("<li><a href=\"#" + id + "\">Cluster Methods</a><ul class=\"nav\">");
          }
          if (list == null) {
            list = new ArrayList<String>();
            markers.put(SERVICE, list);
            list.add("<li><a href=\"#" + id + "\">Table Methods</a><ul class=\"nav\">");
          }
          list.add("<li><a href=\"#" + id + "\">&nbsp;&nbsp;" + name + "</a></li>");
        } else {
          throw new RuntimeException("Unknown name type [" + id + "]");
        }
      }
    }
    List<String> lst = markers.get(SERVICE);
    lst.add("</ul></li>");

    bufferedReader.close();

    inputStream = new FileInputStream(file);
    bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
    line = null;
    boolean process = false;

    FileOutputStream outputStream = new FileOutputStream(args[1]);
    PrintWriter printWriter = new PrintWriter(outputStream);
    printFile(printWriter, "header.html");

    printWriter.println("<div class=\"container bs-docs-container\">");
    printWriter.println("<div class=\"row\">");
    printWriter.println("<div class=\"col-md-3\">");
    printWriter.println("<div class=\"bs-sidebar hidden-print affix\" role=\"complementary\">");
    printWriter.println("<ul class=\"nav bs-sidenav\">");

    List<String> orderedCategories = Arrays.asList("Enumerations", "Data structures", "Service");

    for (String category : orderedCategories) {
      printWriter.println("<li><a href=\"#" + getLink(category) + "\">" + category + "</a>");
      printWriter.println("<ul class=\"nav\">");
      List<String> list = markers.get(category);
      for (String link : list) {
        printWriter.println(link);
      }
      printWriter.println("</ul>");
      printWriter.println("</li>");
    }
    printWriter.println("</ul>");
    printWriter.println("</div>");
    printWriter.println("</div>");
    printWriter.println("<div class=\"col-md-9\" role=\"main\">");
    while ((line = bufferedReader.readLine()) != null) {
      line = line.trim();
      if (line.equals("<hr/><h2 id=\"Enumerations\">Enumerations</h2>")) {
        process = true;
      }
      int index = line.indexOf("</div></body></html>");
      if (index >= 0) {
        line = line.substring(0, index);
      }
      if (line.contains("<br>")) {
        line = line.replace("<br>", "<br/>");
      }
      if (process) {
        if (line.contains("</div>")) {
          line = line.replace("</div>", "</p></section>");
        }
        if (line.contains(DIV_CLASS_DEFINITION)) {
          line = line.replace(DIV_CLASS_DEFINITION, "<section><div class=\"page-header\">");
          line += "</div><p class=\"lead\">";
        }
        printWriter.println(line);
      }

    }
    printWriter.println("</div>");
    printWriter.println("</div>");
    printWriter.println("</div>");
    for (int i = 0; i < 100; i++) {
      printWriter.println("<br/>");
    }
    printFile(printWriter, "footer.html");
    printWriter.close();
    bufferedReader.close();
  }

  private static String getLink(String category) {
    if (category.equals("Service")) {
      return "Svc_Blur";
    } else if (category.equals(STRUCTS)) {
      return "Structs";
    }
    return category;
  }

  private static void printFile(PrintWriter printWriter, String file) throws IOException {
    InputStream inputStream = CreateBlurApiHtmlPage.class.getResourceAsStream(file);
    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
    String line;
    while ((line = bufferedReader.readLine()) != null) {
      printWriter.println(line.trim());
    }
    bufferedReader.close();
  }

}
