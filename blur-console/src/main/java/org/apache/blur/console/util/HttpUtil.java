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

package org.apache.blur.console.util;

import org.apache.commons.io.IOUtils;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class HttpUtil {
  public static final String JSON = "application/json";
  public static final String TEXT = "plain/text";
  public static final String JS = "text/javascript";

  public static void sendResponse(HttpServletResponse res, String body, String contentType) throws IOException {
    res.setContentType(contentType);
    res.setContentLength(body.getBytes().length);
    res.setStatus(HttpServletResponse.SC_OK);
    IOUtils.write(body, res.getOutputStream());
  }

  public static String getFirstParam(String[] param) {
    if (param == null || param.length == 0) {
      return "";
    }
    return param[0];
  }
}
