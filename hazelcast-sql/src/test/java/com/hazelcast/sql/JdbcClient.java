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

package com.hazelcast.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JdbcClient {

    public static void main(String[] args) throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:hazelcast://localhost:10000", "user", "pass");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select age, height from persons where age >= 5 order by age");

        int columnCount = resultSet.getMetaData().getColumnCount();
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; ++i) {
                if (i != 1) {
                    System.out.print('\t');
                }
                System.out.print(resultSet.getObject(i));
            }
            System.out.println();
        }

        resultSet.close();
        statement.close();
        connection.close();
    }

}
