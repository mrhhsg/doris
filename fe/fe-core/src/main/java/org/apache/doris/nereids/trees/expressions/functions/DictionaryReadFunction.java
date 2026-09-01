// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.qe.ConnectContext;

/**
 * A scalar function that reads the values of a dictionary (dict_get, dict_get_many).
 *
 * Dictionaries are authorized like tables of the internal catalog, but they are not relations of
 * the plan, so CheckPrivileges never sees them and no plan/result cache records them as a
 * dependency. ExpressionAnalyzer therefore authorizes such a read once at bind time through
 * {@link #checkReadPrivilege}, and marks the statement so that it is neither served from the sql
 * cache nor reused as a short-circuit point-query plan.
 */
public interface DictionaryReadFunction {
    /**
     * Throw if the current user may not read the dictionary this function refers to. Called before
     * the dictionary is resolved, so a caller without the privilege cannot probe whether it exists.
     */
    void checkReadPrivilege(ConnectContext ctx);
}
