/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.oracle;

import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;
import oracle.jdbc.OracleDriver;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.function.BiFunction;

import static com.google.common.base.Strings.isNullOrEmpty;
import static io.prestosql.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.realColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.timestampWriteFunctionUsingSqlTimestamp;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varbinaryColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class OracleClient
        extends BaseJdbcClient
{
    @Inject
    public OracleClient(BaseJdbcConfig config, OracleConfig oracleConfig)
    {
        super(config, "\"", connectionFactory(config, oracleConfig));
    }

    private static ConnectionFactory connectionFactory(BaseJdbcConfig config, OracleConfig oracleConfig)
    {
        Properties connectionProperties = basicConnectionProperties(config);
        if (!isNullOrEmpty(oracleConfig.getDatabase())) {
            connectionProperties.setProperty("database", oracleConfig.getDatabase());
        }
        if (!isNullOrEmpty(oracleConfig.getInternalLogon())) {
            connectionProperties.setProperty("internal_logon", oracleConfig.getInternalLogon());
        }
        if (oracleConfig.getDefaultRowPrefetch() != null) {
            connectionProperties.setProperty("defaultRowPrefetch", oracleConfig.getDefaultRowPrefetch().toString());
        }
        if (oracleConfig.getRemarksReporting() != null) {
            connectionProperties.setProperty("remarksReporting", oracleConfig.getRemarksReporting().toString());
        }
        if (oracleConfig.getDefaultBatchValue() != null) {
            connectionProperties.setProperty("defaultBatchValue", oracleConfig.getDefaultBatchValue().toString());
        }
        if (oracleConfig.getIncludeSynonyms() != null) {
            connectionProperties.setProperty("includeSynonyms", oracleConfig.getIncludeSynonyms().toString());
        }
        if (oracleConfig.getProcessEscapes() != null) {
            connectionProperties.setProperty("processEscapes", oracleConfig.getProcessEscapes().toString());
        }
        if (oracleConfig.getDefaultNChar() != null) {
            connectionProperties.setProperty("defaultNChar", oracleConfig.getDefaultNChar().toString());
        }
        if (oracleConfig.getUseFetchSizeWithLongColumn() != null) {
            connectionProperties.setProperty("useFetchSizeWithLongColumn", oracleConfig.getUseFetchSizeWithLongColumn().toString());
        }
        if (oracleConfig.getSetFloatAndDoubleUseBinary() != null) {
            connectionProperties.setProperty("SetFloatAndDoubleUseBinary", oracleConfig.getSetFloatAndDoubleUseBinary().toString());
        }

        return new DriverConnectionFactory(
                new OracleDriver(),
                config.getConnectionUrl(),
                Optional.ofNullable(config.getUserCredentialName()),
                Optional.ofNullable(config.getPasswordCredentialName()),
                connectionProperties);
    }

    @Override
    protected Collection<String> listSchemas(Connection connection)
    {
        try (ResultSet resultSet = connection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_SCHEM");
                schemaNames.add(schemaName);
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        String jdbcTypeName = typeHandle.getJdbcTypeName()
                .orElseThrow(() -> new PrestoException(JDBC_ERROR, "Type name is missing: " + typeHandle));

        if (jdbcTypeName.equalsIgnoreCase("BINARY_FLOAT")) {
            return Optional.of(realColumnMapping());
        }
        if (jdbcTypeName.equalsIgnoreCase("BINARY_DOUBLE")) {
            return Optional.of(doubleColumnMapping());
        }
        switch (typeHandle.getJdbcType()) {
            case Types.CLOB:
            case Types.NCLOB:
                return Optional.of(varcharColumnMapping(createUnboundedVarcharType()));

            case Types.BLOB:
                return Optional.of(varbinaryColumnMapping());
        }

        return super.toPrestoType(session, connection, typeHandle);
    }

    @Override
    protected String generateTemporaryTableName()
    {
        UUID uuid = UUID.randomUUID();
        return "tmp_" + Long.toUnsignedString(uuid.getMostSignificantBits(), 32) + Long.toUnsignedString(uuid.getLeastSignificantBits(), 32);
    }

    @Override
    protected void renameTable(JdbcIdentity identity, String catalogName, String schemaName, String tableName, SchemaTableName newTable)
    {
        if (!schemaName.equalsIgnoreCase(newTable.getSchemaName())) {
            throw new PrestoException(NOT_SUPPORTED, "Table rename across schemas is not supported in Oracle");
        }

        try (Connection connection = connectionFactory.openConnection(identity)) {
            String newTableName = newTable.getTableName();
            if (connection.getMetaData().storesUpperCaseIdentifiers()) {
                newTableName = newTableName.toUpperCase(ENGLISH);
            }
            String sql = format(
                    "ALTER TABLE %s RENAME TO %s",
                    quoted(catalogName, schemaName, tableName),
                    quoted(newTableName));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        Optional<String> escape = Optional.ofNullable(metadata.getSearchStringEscape());
        return metadata.getTables(
                null,
                escapeNamePattern(schemaName, escape).orElse(null),
                escapeNamePattern(tableName, escape).orElse(null),
                new String[] {"TABLE", "VIEW", "SYNONYM"});
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (isVarcharType(type)) {
            VarcharType varcharType = (VarcharType) type;
            String dataType;
            if (varcharType.isUnbounded() || varcharType.getBoundedLength() > 4000) {
                dataType = "NCLOB";
            }
            else {
                dataType = "NVARCHAR2(" + varcharType.getBoundedLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }
        if (type instanceof CharType) {
            CharType charType = (CharType) type;
            String dataType;
            if (charType.getLength() > 4000) {
                dataType = "NCLOB";
            }
            else {
                dataType = "NCHAR(" + charType.getLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, charWriteFunction());
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            String dataType = format("NUMBER(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.sliceMapping(dataType, longDecimalWriteFunction(decimalType));
        }
        if (type instanceof BooleanType) {
            return WriteMapping.booleanMapping("NUMBER(1)", booleanWriteFunction());
        }
        if (type instanceof BigintType) {
            return WriteMapping.longMapping("NUMBER(19)", bigintWriteFunction());
        }
        if (type instanceof IntegerType) {
            return WriteMapping.longMapping("NUMBER(10)", integerWriteFunction());
        }
        if (type instanceof SmallintType) {
            return WriteMapping.longMapping("NUMBER(5)", smallintWriteFunction());
        }
        if (type instanceof TinyintType) {
            return WriteMapping.longMapping("NUMBER(3)", tinyintWriteFunction());
        }
        if (type instanceof DoubleType) {
            return WriteMapping.doubleMapping("BINARY_DOUBLE", doubleWriteFunction());
        }
        if (type instanceof RealType) {
            return WriteMapping.longMapping("BINARY_FLOAT", realWriteFunction());
        }
        if (type instanceof VarbinaryType) {
            return WriteMapping.sliceMapping("BLOB", varbinaryWriteFunction());
        }
        if (type instanceof TimestampType) {
            return WriteMapping.longMapping("TIMESTAMP", timestampWriteFunctionUsingSqlTimestamp(session));
        }

        return super.toWriteMapping(session, type);
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> {
            return "SELECT * FROM ( " + sql + " ) WHERE rownum <= " + limit;
        });
    }

    @Override
    public boolean isLimitGuaranteed()
    {
        return true;
    }
}
