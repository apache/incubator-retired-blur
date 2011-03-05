/*
 * Copyright (C) 2011 Near Infinity Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.blur.utils;

import java.util.Comparator;

import com.nearinfinity.blur.thrift.generated.ColumnFamily;

public class ColumnFamilyComparator implements Comparator<ColumnFamily> {

    @Override
    public int compare(ColumnFamily o1, ColumnFamily o2) {
        return o1.family.compareTo(o2.family);
    }

}
