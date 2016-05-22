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
package org.apache.blur.mapreduce.lib.update;

public enum BlurIndexCounter {

  NEW_RECORDS, ROW_IDS_FROM_INDEX, ROW_IDS_TO_UPDATE_FROM_NEW_DATA, ROW_IDS_FROM_NEW_DATA,

  INPUT_FORMAT_MAPPER, INPUT_FORMAT_EXISTING_RECORDS,

  LOOKUP_MAPPER, LOOKUP_MAPPER_EXISTING_RECORDS, LOOKUP_MAPPER_ROW_LOOKUP_ATTEMPT

}
