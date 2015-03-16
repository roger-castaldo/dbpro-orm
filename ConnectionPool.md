#This page describes the parameters to define a connection pool.

# Basic Parameters #

  * **minPoolSize**:  The minimum number of connections to maintain for the connection pool.  (DEFAULT: depends on implementation)
  * **maxPoolSize**:  The maximum number of connections the connection pool will allow.  If obtaining a new connection will require more than this limit an exception will be thrown, but only after multiple attempts to obtain a connection, i.e.  ThreadLock is assumed.  (DEFAULT: depends on implementation)
  * **maxKeepAlive**:  The number of seconds to keep a connection alive before closing it.  If closing it will place the pool count below the limit, a new one will be created to replace it.  (DEFAULT: depends on implementation)
  * **UpdateStructureDebugMode**:  (true/false)  If true the system will not update the structure in the database to match the classes, it will simply output detected differences to the logger debug.  Used for diagnostics purposely mostly.  (DEFAULT: false).
  * **connectionName**:  Defines the name of this connection, if not specified will assume the connection is the default one.  The name is used when specifying the **Table** attribute, to link classes to certain connections, or when accessing the connection from the pool manager.  It must be unique.
  * **allowTableDeletions**:  (true/false)  If true when the system detects tables that are not tied to classes it will delete the table.  Set to false if this behavior is not desired.  (DEFAULT: true)
  * **readTimeout**: Number of seconds before a connection can be considered dead when attempting to execute a query.  After the timeout, the connection will reset itself and attempt the query again for 5 tries before giving up and throwing an exception assuming the connection/server is dead. (DEFAULT: 60)
  * **Readonly**:  (true/false)  If true then the system will not allow inserts, deletes or updates, only selects and it will not make any attempts to scan or change the database structure.  Typically used when linking to external databases not associated with the applications data.  (DEFAULT: false)

# Firebird SQL #

  * **username**:  The username used to authenticate to the server.  (Required)
  * **password**:  The password used to authenticate to the server. (Required)
  * **databasePath**:  The path to the database on the server, either the full path or the aliased path.  (Required)
  * **databaseServer**:  The url/ip address of the database server. (Required)
  * **port**:  The port that the server is configured to listen on.  (DEFAULT: 3050)

**Additional Default Values**:
  * **minPoolSize**: 5
  * **maxPoolSize**: 10
  * **maxKeepAlive**: 300


# MsSQL #

  * **databaseServer**:  The url/ip/common name of the datbase server.  (Required)
  * **port**:  The port that the server is listening on.  (Default: 1433)
  * **database**:  The database the connection is going to use.  (Required)
  * **username**:  The username used to authenticate to the database server.  (Required)
  * **password**:  The password used to authenticate to the database server.  (Required)

**Additional Default Values**
  * **minPoolSize**: 5
  * **maxPoolSize**: 10
  * **maxKeepAlive**: 600


# MySQL #

  * **databaseServer**:  The url/ip of the datbase server.  (Required)
  * **port**:  The port that the server is listening on.  (Default: 3306)
  * **database**:  The database the connection is going to use.  (Required)
  * **username**:  The username used to authenticate to the database server.  (Required)
  * **password**:  The password used to authenticate to the database server.  (Required)

**Additional Default Values**
  * **minPoolSize**: 5
  * **maxPoolSize**: 10
  * **maxKeepAlive**: 600

# PostgreSQL #

  * **databaseServer**:  The url/ip of the datbase server.  (Required)
  * **port**:  The port that the server is listening on.  (Default: 5432)
  * **database**:  The database the connection is going to use.  (Required)
  * **username**:  The username used to authenticate to the database server.  (Required)
  * **password**:  The password used to authenticate to the database server.  (Required)

**Additional Default Values**
  * **minPoolSize**: 5
  * **maxPoolSize**: 10
  * **maxKeepAlive**: 600