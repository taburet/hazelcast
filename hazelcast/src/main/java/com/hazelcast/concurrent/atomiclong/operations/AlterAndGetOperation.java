/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.atomiclong.operations;

import com.hazelcast.concurrent.atomiclong.AtomicLongContainer;
import com.hazelcast.concurrent.atomiclong.AtomicLongDataSerializerHook;
import com.hazelcast.core.IFunction;

public class AlterAndGetOperation extends AbstractAlterOperation {

    public AlterAndGetOperation() {
    }

    public AlterAndGetOperation(String name, IFunction<Long, Long> function) {
        super(name, function);
    }

    @Override
    public void run() throws Exception {
        AtomicLongContainer atomicLongContainer = getLongContainer();

        long input = atomicLongContainer.get();
        long output = function.apply(input);
        shouldBackup = input != output;
        if (shouldBackup) {
            backup = output;
            atomicLongContainer.set(output);
        }

        response = output;
    }

    @Override
    public int getId() {
        return AtomicLongDataSerializerHook.ALTER_AND_GET;
    }
}
