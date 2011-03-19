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

package com.nearinfinity.blur.manager.results;

import java.util.Comparator;

import com.nearinfinity.blur.thrift.generated.BlurResult;

public class BlurResultComparator implements Comparator<BlurResult> {

    @Override
    public int compare(BlurResult o1, BlurResult o2) {
        int compare = Double.compare(o2.score, o1.score);
        if (compare == 0) {
            return o2.locationId.compareTo(o1.locationId);
        }
        return compare;
    }

}
