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

// Reading dictionary values through dict_get()/dict_get_many() requires SELECT on the dictionary,
// checked once at bind time: direct calls and alias function bodies are denied for a user without
// it, persisted views keep the view-only privilege boundary, and neither the sql cache nor a
// reused short-circuit point-query plan may serve dictionary values after the privilege on the
// dictionary was revoked.
suite("test_dictionary_read_auth_cache", "p0,auth_call,nonConcurrent") {
    String user = 'test_dict_read_auth_cache_user'
    String pwd = 'C123_567p'
    String dbName = 'test_dict_read_auth_cache_db'
    String tableName = 'test_dict_read_auth_cache_src'
    String pointTable = 'test_dict_read_auth_cache_pq'
    String dictName = 'test_dict_read_auth_cache_dict'
    String aliasFn = 'test_dict_read_auth_cache_alias'

    String jdbcUrl = context.config.jdbcUrl
    String urlWithoutSchema = jdbcUrl.substring(jdbcUrl.indexOf("://") + 3)
    def sqlIp = urlWithoutSchema.substring(0, urlWithoutSchema.indexOf(":"))
    def sqlPort
    if (urlWithoutSchema.indexOf("/") >= 0) {
        sqlPort = urlWithoutSchema.substring(urlWithoutSchema.indexOf(":") + 1, urlWithoutSchema.indexOf("/"))
    } else {
        sqlPort = urlWithoutSchema.substring(urlWithoutSchema.indexOf(":") + 1)
    }
    def prepareUrl = "jdbc:mysql://" + sqlIp + ":" + sqlPort + "/" + dbName + "?&useServerPrepStmts=true"

    try_sql("DROP USER ${user}")
    try_sql("DROP GLOBAL FUNCTION ${aliasFn}(BIGINT)")
    sql """drop database if exists ${dbName}"""
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    sql """grant select_priv on regression_test to ${user}"""
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO ${user}""";
    }

    sql """create database ${dbName}"""
    sql """
        CREATE TABLE ${dbName}.${tableName} (
            id BIGINT,
            username VARCHAR(30)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
        """
    sql """insert into ${dbName}.${tableName} values (1, 'doris'), (2, 'apache')"""
    // a table that qualifies for short-circuit point queries
    sql """
        CREATE TABLE ${dbName}.${pointTable} (
            id BIGINT,
            v INT
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "store_row_column" = "true"
        );
        """
    sql """insert into ${dbName}.${pointTable} values (1, 10), (2, 20)"""
    sql """
        CREATE DICTIONARY ${dbName}.${dictName} USING ${dbName}.${tableName}
        (
            id KEY,
            username VALUE
        )
        LAYOUT(HASH_MAP)
        PROPERTIES('data_lifetime'='600');
        """
    sql """use ${dbName}"""
    waitDictionaryReady(dictName)

    def readSql = "SELECT dict_get('${dbName}.${dictName}', 'username', id) FROM ${dbName}.${pointTable} WHERE id = 1"
    sql """grant SELECT_PRIV on ${dbName}.${pointTable} to ${user}"""
    sql """grant SELECT_PRIV on ${dbName}.* to ${user}"""

    // 1. sql cache: a dictionary read is never served from the cache, so revoking SELECT on the
    //    dictionary takes effect on the next statement even for an identical, already primed sql.
    withGlobalLock("cache_last_version_interval_second") {
        sql """ADMIN SET ALL FRONTENDS CONFIG ('cache_last_version_interval_second' = '0')"""
        try {
            sql "set enable_sql_cache=true"
            def primed = sql readSql
            assertEquals("doris", primed[0][0])
            primed = sql readSql
            assertEquals("doris", primed[0][0])
            explain {
                sql("physical plan ${readSql}")
                notContains("PhysicalSqlCache")
            }
            connect(user, "${pwd}", context.config.jdbcUrl) {
                sql "set enable_sql_cache=true"
                def userPrimed = sql readSql
                assertEquals("doris", userPrimed[0][0])
                userPrimed = sql readSql
                assertEquals("doris", userPrimed[0][0])
            }
            sql """revoke SELECT_PRIV on ${dbName}.* from ${user}"""
            connect(user, "${pwd}", context.config.jdbcUrl) {
                sql "set enable_sql_cache=true"
                test {
                    sql readSql
                    exception "SELECT command denied"
                }
            }
        } finally {
            sql "set enable_sql_cache=false"
            sql """ADMIN SET ALL FRONTENDS CONFIG ('cache_last_version_interval_second' = '30')"""
        }
    }

    // 2. server prepared statement: a point query reading a dictionary is not short-circuited, so a
    //    later EXECUTE is analyzed (and authorized) again instead of reusing the first plan.
    sql """grant SELECT_PRIV on ${dbName}.* to ${user}"""
    connect(user, "${pwd}", prepareUrl) {
        def stmt = prepareStatement "SELECT dict_get('${dbName}.${dictName}', 'username', id) FROM ${dbName}.${pointTable} WHERE id = ?"
        assertEquals(stmt.class, com.mysql.cj.jdbc.ServerPreparedStatement)
        stmt.setLong(1, 1L)
        def rs = stmt.executeQuery()
        assertTrue(rs.next())
        assertEquals("doris", rs.getString(1))
        rs.close()

        def admin = java.sql.DriverManager.getConnection(context.config.jdbcUrl,
                context.config.jdbcUser, context.config.jdbcPassword)
        try {
            admin.createStatement().execute("revoke SELECT_PRIV on ${dbName}.* from ${user}")
        } finally {
            admin.close()
        }

        stmt.setLong(1, 1L)
        def denied = false
        try {
            stmt.executeQuery()
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("SELECT command denied"), e.getMessage())
            denied = true
        }
        assertTrue(denied)
    }

    // 3. same as 2, but the dictionary read has only literal arguments and BE constant folding is
    //    on, so the read is folded into a literal before the short-circuit rule runs. The reuse
    //    decision must not depend on the plan shape after rewrites.
    sql """grant SELECT_PRIV on ${dbName}.* to ${user}"""
    connect(user, "${pwd}", prepareUrl) {
        sql "set enable_fold_constant_by_be=true"
        def stmt = prepareStatement "SELECT dict_get('${dbName}.${dictName}', 'username', 1) FROM ${dbName}.${pointTable} WHERE id = ?"
        assertEquals(stmt.class, com.mysql.cj.jdbc.ServerPreparedStatement)
        stmt.setLong(1, 1L)
        def rs = stmt.executeQuery()
        assertTrue(rs.next())
        assertEquals("doris", rs.getString(1))
        rs.close()

        def admin = java.sql.DriverManager.getConnection(context.config.jdbcUrl,
                context.config.jdbcUser, context.config.jdbcPassword)
        try {
            admin.createStatement().execute("revoke SELECT_PRIV on ${dbName}.* from ${user}")
        } finally {
            admin.close()
        }

        stmt.setLong(1, 1L)
        def denied = false
        try {
            stmt.executeQuery()
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("SELECT command denied"), e.getMessage())
            denied = true
        }
        assertTrue(denied)
    }

    // 4. persisted views keep Doris's view-only privilege boundary: a user holding SELECT on the
    //    view alone reads dictionary values through it, while a direct call is still denied.
    sql """CREATE VIEW ${dbName}.${dictName}_view AS SELECT dict_get('${dbName}.${dictName}', 'username', 1) AS name"""
    sql """CREATE VIEW ${dbName}.${dictName}_view_many AS SELECT dict_get_many('${dbName}.${dictName}', ['username'], struct(1)) AS r"""
    sql """grant SELECT_PRIV on ${dbName}.${dictName}_view to ${user}"""
    sql """grant SELECT_PRIV on ${dbName}.${dictName}_view_many to ${user}"""
    connect(user, "${pwd}", context.config.jdbcUrl) {
        def viaView = sql """SELECT name FROM ${dbName}.${dictName}_view"""
        assertEquals("doris", viaView[0][0])
        def viaViewMany = sql """SELECT r FROM ${dbName}.${dictName}_view_many"""
        assertTrue(viaViewMany[0][0].toString().contains("doris"), viaViewMany[0][0].toString())
        test {
            sql """SELECT dict_get('${dbName}.${dictName}', 'username', 1)"""
            exception "SELECT command denied"
        }
        test {
            sql """SELECT dict_get_many('${dbName}.${dictName}', ['username'], struct(1))"""
            exception "SELECT command denied"
        }
    }

    // 5. an alias function body is analyzed without a CascadesContext but on behalf of the caller,
    //    so a dictionary read wrapped in one is authorized against the caller as well.
    sql """CREATE GLOBAL ALIAS FUNCTION ${aliasFn}(BIGINT) WITH PARAMETER(k) AS dict_get('${dbName}.${dictName}', 'username', k)"""
    connect(user, "${pwd}", context.config.jdbcUrl) {
        test {
            sql """SELECT ${aliasFn}(1)"""
            exception "SELECT command denied"
        }
    }
    sql """grant SELECT_PRIV on ${dbName}.* to ${user}"""
    connect(user, "${pwd}", context.config.jdbcUrl) {
        def viaAlias = sql """SELECT ${aliasFn}(1)"""
        assertEquals("doris", viaAlias[0][0])
    }
    try_sql("DROP GLOBAL FUNCTION ${aliasFn}(BIGINT)")

    sql """drop database if exists ${dbName}"""
    try_sql("DROP USER ${user}")
}
