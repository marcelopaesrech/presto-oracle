# Presto OraclePlugin

This is a plugin for Presto that allow you to use Oracle Jdbc Connection

[![Presto-Connectors Member](https://img.shields.io/badge/presto--connectors-member-green.svg)](http://presto-connectors.ml)

## Connection Configuration

Create new properties file inside etc/catalog dir:

    connector.name=oracle
    # connection-url must me the URL to access Oracle via JDBC. It can be different depending on your environment.
    # Another example of the URL would be jdbc:oracle:thin:@//ip:port/database. For more information, please go to the JDBC driver docs
    connection-url=jdbc:oracle:thin://ip:port/database
    connection-user=myuser
    connection-password=

Create a dir inside plugin dir called oracle. To make it easier you could copy mysql dir to oracle and remove the mysql-connector and prestodb-mysql jars. Finally put the prestodb-oracle in plugin/oracle folder. Here is the sptes:

    cd $PRESTODB_HOME
    cp -r plugin/mysql plugin/oracle
    rm plugin/oracle/mysql-connector*
    rm plugin/oracle/presto-mysql*
    mv /home/Downloads/presto-oracle*.jar plugin/oracle

## Building Presto Oracle JDBC Plugin

    mvn clean install
    
## Building Oracle Driver
Oracle Driver is not available in common repositories, so you will need to download it from Oracle and install manually in your repository.
