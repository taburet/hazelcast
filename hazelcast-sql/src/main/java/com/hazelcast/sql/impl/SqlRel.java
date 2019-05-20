/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;

import java.util.HashSet;
import java.util.Set;

public interface SqlRel extends RelNode {

    Convention CONVENTION = new Convention.Impl("HAZELCAST", SqlRel.class);

    static <T extends SqlRel> T findInputOf(RelNode node, Class<T> clazz) {
        return findInputOf(node, clazz, new HashSet<>());
    }

    static <T extends SqlRel> T findInputOf(RelNode node, Class<T> clazz, Set<RelNode> visited) {
        if (visited.contains(node)) {
            return null;
        }

        for (RelNode child : node.getInputs()) {
            if (clazz.isInstance(child)) {
                return clazz.cast(child);
            }

            T found = findInputOf(child, clazz, visited);
            if (found != null) {
                return found;
            }
        }

        if (node instanceof RelSubset) {
            RelSubset subset = (RelSubset) node;
            for (RelNode child : subset.getRels()) {
                if (clazz.isInstance(child)) {
                    return clazz.cast(child);
                }

                T found = findInputOf(child, clazz, visited);
                if (found != null) {
                    return found;
                }
            }
        }

        return null;
    }

    void implement(SqlImplementation implementation);

}
