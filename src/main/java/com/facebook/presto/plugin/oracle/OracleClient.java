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
package com.facebook.presto.plugin.oracle;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.Iterables.getOnlyElement;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;
import javax.inject.Inject;

import oracle.jdbc.OracleDriver;

import org.apache.log4j.Logger;

import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * Implementation of OracleClient. It describes table, schemas and columns behaviours.
 * It allows to change the QueryBuilder to a custom one as well.
 * 
 * @author Marcelo Paes Rech
 *
 */
public class OracleClient extends BaseJdbcClient {

	private static final Logger log = Logger.getLogger(OracleClient.class);

	@Inject
	public OracleClient(JdbcConnectorId connectorId, BaseJdbcConfig config,
			OracleConfig oracleConfig) throws SQLException {
		super(connectorId, config, "", new OracleDriver());
		//the empty "" is to not use a quote to create queries
		// BaseJdbcClient already gets these properties
		// connectionProperties.setProperty("user", oracleConfig.getUser());
		// connectionProperties.setProperty("url", oracleConfig.getUrl());
		// connectionProperties.setProperty("password",
		// oracleConfig.getPassword());

	}

	@Override
	public Set<String> getSchemaNames() {
		try (Connection connection = driver.connect(connectionUrl,
				connectionProperties);
				ResultSet resultSet = connection.getMetaData().getSchemas()) {
			ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
			while (resultSet.next()) {
				String schemaName = resultSet.getString(1).toLowerCase();
				log.info("Listing schemas: " + schemaName);
				schemaNames.add(schemaName);
			}
			return schemaNames.build();
		} catch (SQLException e) {
			throw Throwables.propagate(e);
		}
	}

	protected ResultSet getTables(Connection connection, String schemaName,
			String tableName) throws SQLException {
		// Here we put TABLE and SYNONYM when the table schema is another user schema 
		return connection.getMetaData().getTables(null, schemaName, tableName,
				new String[] { "TABLE", "SYNONYM" });
	}

	@Nullable
	@Override
	public JdbcTableHandle getTableHandle(SchemaTableName schemaTableName) {
		try (Connection connection = driver.connect(connectionUrl,
				connectionProperties)) {
			DatabaseMetaData metadata = connection.getMetaData();
			String jdbcSchemaName = schemaTableName.getSchemaName();
			String jdbcTableName = schemaTableName.getTableName();
			if (metadata.storesUpperCaseIdentifiers()) {
				jdbcSchemaName = jdbcSchemaName.toUpperCase();
				jdbcTableName = jdbcTableName.toUpperCase();
			}
			try (ResultSet resultSet = getTables(connection, jdbcSchemaName,
					jdbcTableName)) {
				List<JdbcTableHandle> tableHandles = new ArrayList<>();
				while (resultSet.next()) {
					tableHandles.add(new JdbcTableHandle(connectorId,
							schemaTableName, resultSet.getString("TABLE_CAT"),
							resultSet.getString("TABLE_SCHEM"), resultSet
									.getString("TABLE_NAME")));
				}
				if (tableHandles.isEmpty()) {
					return null;
				}
				if (tableHandles.size() > 1) {
					throw new PrestoException(NOT_SUPPORTED,
							"Multiple tables matched: " + schemaTableName);
				}
				return getOnlyElement(tableHandles);
			}
		} catch (SQLException e) {
			throw Throwables.propagate(e);
		}
	}

	@Override
	public List<JdbcColumnHandle> getColumns(JdbcTableHandle tableHandle) {
		try (Connection connection = driver.connect(connectionUrl,
				connectionProperties)) {
			//If the table is mapped to another user you will need to get the synonym to that table
			//So, in this case, is mandatory to use setIncludeSynonyms
			( (oracle.jdbc.driver.OracleConnection)connection ).setIncludeSynonyms(true);
 			DatabaseMetaData metadata = connection.getMetaData();
			String schemaName = tableHandle.getSchemaName().toUpperCase();
			String tableName = tableHandle.getTableName().toUpperCase();
			try (ResultSet resultSet = metadata.getColumns(null, schemaName,
					tableName, null)) {
				List<JdbcColumnHandle> columns = new ArrayList<>();
				boolean found = false;
				while (resultSet.next()) {
					found = true;
					Type columnType = toPrestoType(resultSet
                            .getInt("DATA_TYPE"), resultSet.getInt("COLUMN_SIZE"));
					// skip unsupported column types
					if (columnType != null) {
						String columnName = resultSet.getString("COLUMN_NAME");
						columns.add(new JdbcColumnHandle(connectorId,
								columnName, columnType));
					}
				}
				if (!found) {
					throw new TableNotFoundException(
							tableHandle.getSchemaTableName());
				}
				if (columns.isEmpty()) {
					throw new PrestoException(NOT_SUPPORTED,
							"Table has no supported column types: "
									+ tableHandle.getSchemaTableName());
				}
				return ImmutableList.copyOf(columns);
			}
		} catch (SQLException e) {
			throw Throwables.propagate(e);
		}
	}

	@Override
	public List<SchemaTableName> getTableNames(@Nullable String schema) {
		try (Connection connection = driver.connect(connectionUrl,
				connectionProperties)) {
			DatabaseMetaData metadata = connection.getMetaData();
			if (metadata.storesUpperCaseIdentifiers() && (schema != null)) {
				schema = schema.toUpperCase();
			}
			try (ResultSet resultSet = getTables(connection, schema, null)) {
				ImmutableList.Builder<SchemaTableName> list = ImmutableList
						.builder();
				while (resultSet.next()) {
					list.add(getSchemaTableName(resultSet));
				}
				return list.build();
			}
		} catch (SQLException e) {
			throw Throwables.propagate(e);
		}
	}

	@Override
	protected SchemaTableName getSchemaTableName(ResultSet resultSet)
			throws SQLException {
		String tableSchema = resultSet.getString("TABLE_SCHEM");
		String tableName = resultSet.getString("TABLE_NAME");
		if (tableSchema != null) {
			tableSchema = tableSchema.toLowerCase();
		}
		if (tableName != null) {
			tableName = tableName.toLowerCase();
		}
		return new SchemaTableName(tableSchema, tableName);
	}

	@Override
	protected String toSqlType(Type type) {
		//just for debug
		String sqlType = super.toSqlType(type);
		return sqlType;
	}
}
