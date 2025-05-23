/*
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
package openwhisk

import "log"

// Debugging flag
var Debugging = true

// Debug emits a debug message
func Debug(format string, args ...interface{}) {
	if Debugging {
		log.Printf(format, args...)
	}
}

// DebugLimit emits a debug message with a limit in lenght
func DebugLimit(msg string, in []byte, limit int) {
	if len(in) < limit {
		Debug("%s:%s", msg, in)
	} else {
		Debug("%s:%s...", msg, in[0:limit])
	}
}
