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

/**
 * To get the custom properties to connect to the database. User, password and
 * URL is provided by de BaseJdbcClient is not required. If there is another
 * custom configuration it should be put in here.
 */
public class OracleConfig
{
    private String user;

    private String password;

    private String url;

    public String getUser()
    {
        return user;
    }

    @Config("oracle.user")
    public OracleConfig setUser(String user)
    {
        this.user = user;
        return this;
    }

    public String getPassword()
    {
        return password;
    }

    @Config("oracle.password")
    public OracleConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    public String getUrl()
    {
        return url;
    }

    @Config("oracle.password")
    public OracleConfig setUrl(String url)
    {
        this.url = url;
        return this;
    }
}
