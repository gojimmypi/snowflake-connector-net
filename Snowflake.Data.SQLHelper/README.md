# Microsoft.ApplicationBlocks.Data

The [original SQLHelper](../Microsoft.ApplicationBlocks.Data/) in C#, Converted to Snowflake.

Simple, direct SQL access without the hidden complexity and abstraction of things like Entity Framework.

Note: the [master branch](https://github.com/gojimmypi/snowflake-connector-net) of this fork, includes the 
[Snowflake.Data.SQLHelper](https://github.com/gojimmypi/snowflake-connector-net/tree/master/Snowflake.Data.SQLHelper)
and does *not* currently work for parameterized `SnowflakeDbDataReader ExecuteReader` calls from the [upstream master](https://github.com/snowflakedb/snowflake-connector-net)! 
See [snowflake-connector-net/issues/285](https://github.com/snowflakedb/snowflake-connector-net/issues/285).
It was hoped that the simple PR fix would be promptly accepted.

For example:

## ExecuteScalar

```
int rowcount = (long)SnowflakeSqlHelper.ExecuteScalar(SnowflakeHelper.ConnectionString, CommandType.Text, "select count(*) from mytable");
```
## ExecuteDataset

Fetch a set of records into local `DataSet` object

```
        static void SnowflakeTest()
        {
            string sql = " SET myvar='(?)';  SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=$myvar OR COLUMN_NAME=$myvar; ";

            // note parameters are named 1, 2, etc... based on ordinal reference of "(?)"s found in sql string
            SnowflakeDbParameter[] sqlParams = new SnowflakeDbParameter[]
            {
                 new SnowflakeDbParameter("1", Snowflake.Data.Core.SFDataType.TEXT),  
                 // new SnowflakeDbParameter("2", Snowflake.Data.Core.SFDataType.TEXT),  
            };
            sqlParams[0].Value = "VW_ALL_IIS_LHJ";                     // keep track of the user doing the upload
            DataSet ds = SnowflakeSqlHelper.ExecuteDataset(SnowflakeHelper.ConnectionString, CommandType.Text, sql, sqlParams);

        }
```

## Bulk Insert

This is an example to bulk insert int oa local workspace table, and then upon success rename to the actual target table. 
The Snowflake `SELECT` statement is supplied in FromFileSQL, and is assumed to be exactly mapped to `ToTable` (typically a view).

```
        private static void SnowflakeFetch(string FromFileSQL, string ToServer, string ToDatabase, string ToTable, bool TruncateToTable, bool InPlaceRename )
        {
            bool OkToProceed = true;
            LogHelper.WriteLine("Reading SQL Statement for Snowflake Query from {0}", FromFileSQL);

            // we assume the sql text here exactly matches the type and ordinal reference in ToTable (typically a view)
            string sql = System.IO.File.ReadAllText(FromFileSQL);

            // note parameters are named 1, 2, etc... based on ordinal reference of "(?)"s found in sql string
            SnowflakeDbParameter[] sqlParams = new SnowflakeDbParameter[]
            {
                 new SnowflakeDbParameter("1", Snowflake.Data.Core.SFDataType.TEXT), // create_app_user_id
                 new SnowflakeDbParameter("2", Snowflake.Data.Core.SFDataType.TEXT), // session_id
            };
            sqlParams[0].Value = Environment.UserName;                     // keep track of the user doing the upload
            sqlParams[1].Value = GetUploadSessionID(ToServer, ToDatabase); // in case there are concurrent uploads, keep track by this ID (see dbo.upload_session)

            DateTime start = DateTime.Now;
            LogHelper.WriteLine("Starting bulk transfer {0}", start.ToLongTimeString());

            using (SnowflakeDbDataReader r = SnowflakeSqlHelper.ExecuteReader(SnowflakeHelper.ConnectionString, CommandType.Text, sql, sqlParams))
            {
                LogHelper.WriteLine("SnowflakeDbDataReader stream opened.");
                // optionally truncate target table
                if (TruncateToTable)
                {
                    OkToProceed = TruncateTable(
                                                    ToServer: ServerName,
                                                    ToDatabase: DatabaseName,
                                                    ToTable: TableName
                                                 );
                }

                // if all is well, proceed to bulk copy
                if (OkToProceed)
                {
                    try
                    {
                        // var options = SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.KeepNulls; // | SqlBulkCopyOptions.FireTriggers; // SqlBulkCopyOptions.CheckConstraints | SqlBulkCopyOptions.KeepIdentity;
                        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(TrustedConnectionString(ToServer, ToDatabase)))
                        {
                            bulkCopy.DestinationTableName = ToTable;
                            bulkCopy.BulkCopyTimeout = SQL_TIMEOUT;
                            LogHelper.WriteLine("Begin actual bulk insert...");
                            bulkCopy.WriteToServer(r);
                            LogHelper.WriteLine("Bulk insert complete!");
                        }
                    }
                    catch (Exception ex)
                    {
                        OkToProceed = false;
                        LogHelper.WriteErrorLine("ERROR: Failed bulk copy insert.");
                        LogHelper.WriteErrorLine(ex.Message);
                        LogHelper.WriteExceptionMessage(ex);
                    }
                }

                if (OkToProceed && InPlaceRename)
                {
                    OkToProceed = ReplaceTable(
                                                    ToServer: ServerName,
                                                    ToDatabase: DatabaseName,
                                                    ToTable: TableName
                                                 );
                }

            }
            System.TimeSpan diffResult = (DateTime.Now).Subtract(start);
            LogHelper.WriteLine("Download time={0:N1}", diffResult.TotalMinutes);
        }
```