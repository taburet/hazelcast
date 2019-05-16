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

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.sql.parser.SqlParser;

public class SqlPrepare extends CalcitePrepareImpl {

    @Override
    protected SqlParser.ConfigBuilder createParserConfig() {
        SqlParser.ConfigBuilder parserConfig = super.createParserConfig();
        parserConfig.setUnquotedCasing(Casing.UNCHANGED);
        parserConfig.setQuotedCasing(Casing.UNCHANGED);
        parserConfig.setCaseSensitive(true);
        return parserConfig;
    }

    @Override
    protected RelOptPlanner createPlanner(Context prepareContext, org.apache.calcite.plan.Context externalContext,
                                          RelOptCostFactory costFactory) {
        RelOptPlanner planner = super.createPlanner(prepareContext, externalContext, costFactory);
        planner.addRule(SqlToEnumerableConverterRule.INSTANCE);
        planner.addRule(SqlProjectRule.INSTANCE);
        return planner;
    }

}
