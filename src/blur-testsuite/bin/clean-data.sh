#!/usr/bin/env bash

# Copyright (C) 2011 Near Infinity Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/blur-testsuite-config.sh

$bin/stop-blur.sh
ssh blur@$BLUR_VM_IP /home/blur/zookeeper-3.3.4-cdh3u3/bin/zkServer.sh stop
ssh blur@$BLUR_VM_IP /home/blur/hadoop-0.20.2-cdh3u3/bin/stop-dfs.sh

# Removing zookeeper data
ssh blur@$BLUR_VM_IP rm -r "/home/blur/zookeeper-3.3.4-cdh3u3/data/*"

# Removing hadoop data
ssh blur@$BLUR_VM_IP rm -r "/home/blur/hadoop-0.20.2-cdh3u3/dfs/*"

ssh blur@$BLUR_VM_IP /home/blur/hadoop-0.20.2-cdh3u3/bin/hadoop namenode -format

$bin/start-blur.sh