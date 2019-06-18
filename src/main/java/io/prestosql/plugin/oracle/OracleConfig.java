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

import io.airlift.configuration.Config;

public class OracleConfig
{
    private String database;

    private String internalLogon;

    private Integer defaultRowPrefetch;

    private Boolean remarksReporting;

    private Integer defaultBatchValue;

    private Boolean includeSynonyms;

    private Boolean processEscapes;

    private Boolean defaultNChar;

    private Boolean useFetchSizeWithLongColumn;

    private Boolean setFloatAndDoubleUseBinary;

    public String getDatabase()
    {
        return database;
    }

    @Config("oracle.database")
    public OracleConfig setDatabase(String database)
    {
        this.database = database;
        return this;
    }

    public String getInternalLogon()
    {
        return internalLogon;
    }

    @Config("oracle.internal-logon")
    public OracleConfig setInternalLogon(String internalLogon)
    {
        this.internalLogon = internalLogon;
        return this;
    }

    public Integer getDefaultRowPrefetch()
    {
        return defaultRowPrefetch;
    }

    @Config("oracle.default-row-prefetch")
    public OracleConfig setDefaultRowPrefetch(Integer defaultRowPrefetch)
    {
        this.defaultRowPrefetch = defaultRowPrefetch;
        return this;
    }

    public Boolean getRemarksReporting()
    {
        return remarksReporting;
    }

    @Config("oracle.remarks-reporting")
    public OracleConfig setRemarksReporting(Boolean remarksReporting)
    {
        this.remarksReporting = remarksReporting;
        return this;
    }

    public Integer getDefaultBatchValue()
    {
        return defaultBatchValue;
    }

    @Config("oracle.default-batch-value")
    public OracleConfig setDefaultBatchValue(Integer defaultBatchValue)
    {
        this.defaultBatchValue = defaultBatchValue;
        return this;
    }

    public Boolean getIncludeSynonyms()
    {
        return includeSynonyms;
    }

    @Config("oracle.include-synonyms")
    public OracleConfig setIncludeSynonyms(Boolean includeSynonyms)
    {
        this.includeSynonyms = includeSynonyms;
        return this;
    }

    public Boolean getProcessEscapes()
    {
        return processEscapes;
    }

    @Config("oracle.process-escapes")
    public OracleConfig setProcessEscapes(Boolean processEscapes)
    {
        this.processEscapes = processEscapes;
        return this;
    }

    public Boolean getDefaultNChar()
    {
        return defaultNChar;
    }

    @Config("oracle.default-nchar")
    public OracleConfig setDefaultNChar(Boolean defaultNChar)
    {
        this.defaultNChar = defaultNChar;
        return this;
    }

    public Boolean getUseFetchSizeWithLongColumn()
    {
        return useFetchSizeWithLongColumn;
    }

    @Config("oracle.use-fetch-size-with-long-column")
    public OracleConfig setUseFetchSizeWithLongColumn(Boolean useFetchSizeWithLongColumn)
    {
        this.useFetchSizeWithLongColumn = useFetchSizeWithLongColumn;
        return this;
    }

    public Boolean getSetFloatAndDoubleUseBinary()
    {
        return setFloatAndDoubleUseBinary;
    }

    @Config("oracle.set-float-and-double-use-binary")
    public OracleConfig setSetFloatAndDoubleUseBinary(Boolean setFloatAndDoubleUseBinary)
    {
        this.setFloatAndDoubleUseBinary = setFloatAndDoubleUseBinary;
        return this;
    }
}
