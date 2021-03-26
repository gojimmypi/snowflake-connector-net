//===============================================================================
// Microsoft Data Access Application Block for .NET
// http://msdn.microsoft.com/library/en-us/dnbda/html/daab-rm.asp
//
// from https://web.archive.org/web/20120807083158/http://www.sharpdeveloper.net/source/SqlHelper-Source-Code-cs.html
//
// SQLHelper.cs
//
// This file contains the implementations of the SqlHelper and SqlHelperParameterCache
// classes.
//
// For more information see the Data Access Application Block Implementation Overview. 
// 
//===============================================================================
// Copyright (C) 2000-2001 Microsoft Corporation
// All rights reserved.
// THIS CODE AND INFORMATION IS PROVIDED "AS IS" WITHOUT WARRANTY
// OF ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT
// LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR
// FITNESS FOR A PARTICULAR PURPOSE.
//==============================================================================
//
// Converted to use with Snowflake.Data by gojimmypi March 25, 2021
//  
// https://github.com/gojimmypi/snowflake-connector-net
//


using System;
using System.Data;
using System.Xml;
using System.Data.SqlClient; // NuGet package
using System.Collections;
using Snowflake.Data.Client; // Often a NuGet package, but a project reference here


//Microsoft.ApplicationBlocks.Data.NetCore.Snowflake

//Installing:

//Microsoft.NETCore.Platforms.3.1.0
//Microsoft.Win32.Registry.4.7.0
//runtime.native.System.Data.SqlClient.sni.4.7.0
//runtime.win - arm64.runtime.native.System.Data.SqlClient.sni.4.4.0
//runtime.win - x64.runtime.native.System.Data.SqlClient.sni.4.4.0
//runtime.win - x86.runtime.native.System.Data.SqlClient.sni.4.4.0
//System.Data.SqlClient.4.8.2
//System.Security.AccessControl.4.7.0
//System.Security.Principal.Windows.4.7.0


//Microsoft.ApplicationBlocks.Data.NetCore.Snowflake

//Installing:

//AngleSharp .0.12.1
//log4net .2.0.11
//Microsoft.CSharp.4.5.0
//Microsoft.IdentityModel.JsonWebTokens.6.8.0
//Microsoft.IdentityModel.Logging.6.8.0
//Microsoft.IdentityModel.Tokens.6.8.0
//Microsoft.NETCore.Targets.1.1.3
//Newtonsoft.Json.11.0.2
//Portable.BouncyCastle.1.8.9
//Snowflake.Data.1.2.0
//System.Configuration.ConfigurationManager.4.5.0
//System.IdentityModel.Tokens.Jwt.6.8.0
//System.Runtime.4.3.1
//System.Runtime.CompilerServices.Unsafe.4.5.0
//System.Security.Cryptography.Cng.4.5.0
//System.Security.Cryptography.ProtectedData.4.5.0
//System.Security.Permissions.4.5.0
//System.Text.Encoding.CodePages.4.5.0
//System.Text.RegularExpressions.4.3.1


namespace Snowflake.Data
{
    /// <summary>
    /// The SqlHelper class is intended to encapsulate high performance, scalable best practices for 
    /// common uses of SqlClient.
    /// </summary>
    public sealed class SnowflakeSqlHelper
    {
        #region private utility methods & constructors

        //Since this class provides only static methods, make the default constructor private to prevent 
        //instances from being created with "new SqlHelper()".
        private SnowflakeSqlHelper() { }



        /// <summary>
        /// This method is used to attach array of SnowflakeDbParameters to a SnowflakeDbCommand.
        /// 
        /// This method will assign a value of DbNull to any parameter with a direction of
        /// InputOutput and a value of null.  
        /// 
        /// This behavior will prevent default values from being used, but
        /// this will be the less common case than an intended pure output parameter (derived as InputOutput)
        /// where the user provided no input value.
        /// </summary>
        /// <param name="command">The command to which the parameters will be added</param>
        /// <param name="commandParameters">an array of SnowflakeDbParameters tho be added to command</param>
        private static void AttachParameters(System.Data.Common.DbCommand command, SnowflakeDbParameter[] commandParameters)
        {
            foreach (SnowflakeDbParameter p in commandParameters)
            {
                //check for derived output value with no value assigned
                if ((p.Direction == ParameterDirection.InputOutput) && (p.Value == null))
                {
                    p.Value = DBNull.Value;
                }

                command.Parameters.Add(p);
            }
        }

        /// <summary>
        /// This method assigns an array of values to an array of SnowflakeDbParameters.
        /// </summary>
        /// <param name="commandParameters">array of SnowflakeDbParameters to be assigned values</param>
        /// <param name="parameterValues">array of objects holding the values to be assigned</param>
        private static void AssignParameterValues(SnowflakeDbParameter[] commandParameters, object[] parameterValues)
        {
            if ((commandParameters == null) || (parameterValues == null))
            {
                //do nothing if we get no data
                return;
            }

            // we must have the same number of values as we pave parameters to put them in
            if (commandParameters.Length != parameterValues.Length)
            {
                throw new ArgumentException("Parameter count does not match Parameter Value count.");
            }

            //iterate through the SnowflakeDbParameters, assigning the values from the corresponding position in the 
            //value array
            for (int i = 0, j = commandParameters.Length; i < j; i++)
            {
                commandParameters[i].Value = parameterValues[i];
            }
        }

        /// <summary>
        /// This method opens (if necessary) and assigns a connection, transaction, command type and parameters 
        /// to the provided command.
        /// </summary>
        /// <param name="command">the SnowflakeDbCommand to be prepared</param>
        /// <param name="connection">a valid SnowflakeDbConnection, on which to execute this command</param>
        /// <param name="transaction">a valid SnowflakeDbTransaction, or 'null'</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <param name="commandParameters">an array of SnowflakeDbParameters to be associated with the command or 'null' if no parameters are required</param>
        private static void PrepareCommand(System.Data.Common.DbCommand command, System.Data.Common.DbConnection connection, SnowflakeDbTransaction transaction, CommandType commandType, string commandText, SnowflakeDbParameter[] commandParameters)
        {
            //if the provided connection is not open, we will open it
            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }

            //associate the connection with the command
            command.Connection = connection;

            //set the command text (stored procedure name or SQL statement)
            command.CommandText = commandText;

            //if we were provided a transaction, assign it.
            if (transaction != null)
            {
                command.Transaction = transaction;
            }

            //set the command type
            command.CommandType = commandType;

            //attach the command parameters if they are provided
            if (commandParameters != null)
            {
                AttachParameters(command, commandParameters);
            }

            return;
        }


        #endregion private utility methods & constructors

        #region ExecuteNonQuery

        // Jan 20, 2007 - Sadeer
        // Added the ExecuteDeleteQuery method to support the History (Audit Trail) feature

        /// <summary>
        /// Execute a SQL delete statement against the database specified in the connection string. 
        /// </summary>
        /// <param name="connectionString">The given connection string to connect to the database</param>
        /// <param name="deleteText">The SQL DELETE statement to execute</param>
        /// <param name="userID">The 24/7 ID of the user invoking the DELETE</param>
        /// <returns>An integer representing the number of rows affected by the DELETE command</returns>
        public static int ExecuteDeleteQuery(string connectionString, string deleteText, int userID)
        {
            // This function assumes that the given SQL query is actually a SQL DELETE statement.
            string strDeleteText = deleteText.Trim().ToUpper();
            if (0 != strDeleteText.IndexOf("DELETE")) return -1;

            // DELETE FROM table WHERE c1 = v1 AND ...
            int iStartIndex = strDeleteText.IndexOf("FROM") + "FROM ".Length;
            int iEndIndex = strDeleteText.IndexOf("WHERE");

            string strTableName = strDeleteText.Substring(iStartIndex, iEndIndex - iStartIndex);

            // Prepare the UPDATE statement
            string strUpdateText = @"UPDATE " + strTableName +
                @" SET [TimeStamp] = GetDate() AND [User_ID] = @UserID";

            //SnowflakeDbParameter oParam = new SnowflakeDbParameter("@UserID", userID);

            // Execute the UPDATE statement
            // ExecuteNonQuery(connectionString, CommandType.Text, strUpdateText, oParam);

            // Finally, execute the DELETE statement
            // return ExecuteNonQuery(connectionString, CommandType.Text, deleteText, oParam);

            throw new Exception("ExecuteDeleteQuery not implemented");
        }

        // End - Jan 20, 2007

        // Jan 29, 2007 - Sadeer

        /// <summary>
        /// Execute a SQL delete statement against the database specified in the transaction. 
        /// </summary>
        /// <param name="transaction">The transaction this DELETE statement will be part of</param>
        /// <param name="deleteText">The SQL DELETE statement to execute</param>
        /// <param name="userID">The 24/7 ID of the user invoking the DELETE</param>
        /// <returns>An integer representing the number of rows affected by the DELETE command</returns>
        public static int ExecuteDeleteQuery(SnowflakeDbTransaction transaction, string deleteText, int userID)
        {
            // This function assumes that the given SQL query is actually a SQL DELETE statement.
            string strDeleteText = deleteText.Trim().ToUpper();
            if (0 != strDeleteText.IndexOf("DELETE")) return -1;

            // DELETE FROM table WHERE c1 = v1 AND ...
            int iStartIndex = strDeleteText.IndexOf("FROM") + "FROM ".Length;
            int iEndIndex = strDeleteText.IndexOf("WHERE");

            string strTableName = strDeleteText.Substring(iStartIndex, iEndIndex - iStartIndex);

            // Prepare the UPDATE statement
            string strUpdateText = @"UPDATE " + strTableName +
                @" SET [TimeStamp] = GetDate() AND [User_ID] = @UserID";

            // SnowflakeDbParameter oParam = new SnowflakeDbParameter("@UserID", userID);

            // Finally, execute the DELETE statement
            // return ExecuteNonQuery(transaction, CommandType.Text, deleteText, oParam);

            throw new Exception("ExecuteDeleteQuery not implemented");

        }

        // End - Jan 29, 2007

        /// <summary>
        /// Execute a SnowflakeDbCommand (that returns no resultset and takes no parameters) against the database specified in 
        /// the connection string. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int result = ExecuteNonQuery(connString, CommandType.StoredProcedure, "PublishOrders");
        /// </remarks>
        /// <param name="connectionString">a valid connection string for a SnowflakeDbConnection</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <returns>an int representing the number of rows affected by the command</returns>
        public static int ExecuteNonQuery(string connectionString, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SnowflakeDbParameters
            return ExecuteNonQuery(connectionString, commandType, commandText, (SnowflakeDbParameter[])null);
        }

        public static SnowflakeDbConnection SnowflakeDbConnectionSnowflake(string connection)
        {
            SnowflakeDbConnection c = new SnowflakeDbConnection();
            c.ConnectionString = connection;
            return c;
        }

        /// <summary>
        /// Execute a SnowflakeDbCommand (that returns no resultset) against the database specified in the connection string 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int result = ExecuteNonQuery(connString, CommandType.StoredProcedure, "PublishOrders", new SnowflakeDbParameter("@prodid", 24));
        /// </remarks>
        /// <param name="connectionString">a valid connection string for a SnowflakeDbConnection</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <param name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>an int representing the number of rows affected by the command</returns>
        public static int ExecuteNonQuery(string connectionString, CommandType commandType, string commandText, params SnowflakeDbParameter[] commandParameters)
        {
            //create & open a SnowflakeDbConnection, and dispose of it after we are done.
            using (SnowflakeDbConnection cn = new SnowflakeDbConnection())
            {
                cn.ConnectionString = connectionString;
                cn.Open();

                //call the overload that takes a connection in place of the connection string
                return ExecuteNonQuery(cn, commandType, commandText, commandParameters);
            }
        }

        /// <summary>
        /// Execute a stored procedure via a SnowflakeDbCommand (that returns no resultset) against the database specified in 
        /// the connection string using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// </summary>
        /// <remarks>
        /// This method provides no access to output parameters or the stored procedure's return value parameter.
        /// 
        /// e.g.:  
        ///  int result = ExecuteNonQuery(connString, "PublishOrders", 24, 36);
        /// </remarks>
        /// <param name="connectionString">a valid connection string for a SnowflakeDbConnection</param>
        /// <param name="spName">the name of the stored prcedure</param>
        /// <param name="parameterValues">an array of objects to be assigned as the input values of the stored procedure</param>
        /// <returns>an int representing the number of rows affected by the command</returns>
        public static int ExecuteNonQuery(string connectionString, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                //pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                SnowflakeDbParameter[] commandParameters = SqlHelperParameterCache.GetSpParameterSet(connectionString, spName);

                //assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, parameterValues);

                //call the overload that takes an array of SnowflakeDbParameters
                return ExecuteNonQuery(connectionString, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteNonQuery(connectionString, CommandType.StoredProcedure, spName);
            }
        }

        /// <summary>
        /// Execute a SnowflakeDbCommand (that returns no resultset and takes no parameters) against the provided SnowflakeDbConnection. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int result = ExecuteNonQuery(conn, CommandType.StoredProcedure, "PublishOrders");
        /// </remarks>
        /// <param name="connection">a valid SnowflakeDbConnection</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <returns>an int representing the number of rows affected by the command</returns>
        public static int ExecuteNonQuery(SnowflakeDbConnection connection, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SnowflakeDbParameters
            return ExecuteNonQuery(connection, commandType, commandText, (SnowflakeDbParameter[])null);
        }

        /// <summary>
        /// Execute a SnowflakeDbCommand (that returns no resultset) against the specified SnowflakeDbConnection 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int result = ExecuteNonQuery(conn, CommandType.StoredProcedure, "PublishOrders", new SnowflakeDbParameter("@prodid", 24));
        /// </remarks>
        /// <param name="connection">a valid SnowflakeDbConnection</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <param name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>an int representing the number of rows affected by the command</returns>
        public static int ExecuteNonQuery(SnowflakeDbConnection connection, CommandType commandType, string commandText, params SnowflakeDbParameter[] commandParameters)
        {
            //create a command and prepare it for execution
            SnowflakeDbCommand cmd = (SnowflakeDbCommand)connection.CreateCommand();
            PrepareCommand(cmd, connection, (SnowflakeDbTransaction)null, commandType, commandText, commandParameters);

            //finally, execute the command.
            int retval = cmd.ExecuteNonQuery();

            // detach the SnowflakeDbParameters from the command object, so they can be used again.
            cmd.Parameters.Clear();
            return retval;
        }

        /// <summary>
        /// Execute a stored procedure via a SnowflakeDbCommand (that returns no resultset) against the specified SnowflakeDbConnection 
        /// using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// </summary>
        /// <remarks>
        /// This method provides no access to output parameters or the stored procedure's return value parameter.
        /// 
        /// e.g.:  
        ///  int result = ExecuteNonQuery(conn, "PublishOrders", 24, 36);
        /// </remarks>
        /// <param name="connection">a valid SnowflakeDbConnection</param>
        /// <param name="spName">the name of the stored procedure</param>
        /// <param name="parameterValues">an array of objects to be assigned as the input values of the stored procedure</param>
        /// <returns>an int representing the number of rows affected by the command</returns>
        public static int ExecuteNonQuery(SnowflakeDbConnection connection, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                //pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                SnowflakeDbParameter[] commandParameters = SqlHelperParameterCache.GetSpParameterSet(connection.ConnectionString, spName);

                //assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, parameterValues);

                //call the overload that takes an array of SnowflakeDbParameters
                return ExecuteNonQuery(connection, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteNonQuery(connection, CommandType.StoredProcedure, spName);
            }
        }

        /// <summary>
        /// Execute a SnowflakeDbCommand (that returns no resultset and takes no parameters) against the provided SnowflakeDbTransaction. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int result = ExecuteNonQuery(trans, CommandType.StoredProcedure, "PublishOrders");
        /// </remarks>
        /// <param name="transaction">a valid SnowflakeDbTransaction</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <returns>an int representing the number of rows affected by the command</returns>
        public static int ExecuteNonQuery(SnowflakeDbTransaction transaction, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SnowflakeDbParameters
            return ExecuteNonQuery(transaction, commandType, commandText, (SnowflakeDbParameter[])null);
        }

        /// <summary>
        /// Execute a SnowflakeDbCommand (that returns no resultset) against the specified SnowflakeDbTransaction
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int result = ExecuteNonQuery(trans, CommandType.StoredProcedure, "GetOrders", new SnowflakeDbParameter("@prodid", 24));
        /// </remarks>
        /// <param name="transaction">a valid SnowflakeDbTransaction</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <param name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>an int representing the number of rows affected by the command</returns>
        public static int ExecuteNonQuery(SnowflakeDbTransaction transaction, CommandType commandType, string commandText, params SnowflakeDbParameter[] commandParameters)
        {
            //create a command and prepare it for execution
            SnowflakeDbCommand cmd = (SnowflakeDbCommand)transaction.Connection.CreateCommand();
            PrepareCommand(cmd, transaction.Connection, transaction, commandType, commandText, commandParameters);

            //finally, execute the command.
            int retval = cmd.ExecuteNonQuery();

            // detach the SnowflakeDbParameters from the command object, so they can be used again.
            cmd.Parameters.Clear();
            return retval;
        }

        /// <summary>
        /// Execute a stored procedure via a SnowflakeDbCommand (that returns no resultset) against the specified 
        /// SnowflakeDbTransaction using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// </summary>
        /// <remarks>
        /// This method provides no access to output parameters or the stored procedure's return value parameter.
        /// 
        /// e.g.:  
        ///  int result = ExecuteNonQuery(conn, trans, "PublishOrders", 24, 36);
        /// </remarks>
        /// <param name="transaction">a valid SnowflakeDbTransaction</param>
        /// <param name="spName">the name of the stored procedure</param>
        /// <param name="parameterValues">an array of objects to be assigned as the input values of the stored procedure</param>
        /// <returns>an int representing the number of rows affected by the command</returns>
        public static int ExecuteNonQuery(SnowflakeDbTransaction transaction, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                //pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                SnowflakeDbParameter[] commandParameters = SqlHelperParameterCache.GetSpParameterSet(transaction.Connection.ConnectionString, spName);

                //assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, parameterValues);

                //call the overload that takes an array of SnowflakeDbParameters
                return ExecuteNonQuery(transaction, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteNonQuery(transaction, CommandType.StoredProcedure, spName);
            }
        }


        #endregion ExecuteNonQuery

        #region ExecuteDataSet

        /// <summary>
        /// Execute a SnowflakeDbCommand (that returns a resultset and takes no parameters) against the database specified in 
        /// the connection string. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  DataSet ds = ExecuteDataset(connString, CommandType.StoredProcedure, "GetOrders");
        /// </remarks>
        /// <param name="connectionString">a valid connection string for a SnowflakeDbConnection</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <returns>a dataset containing the resultset generated by the command</returns>
        public static DataSet ExecuteDataset(string connectionString, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SnowflakeDbParameters
            return ExecuteDataset(connectionString, commandType, commandText, (SnowflakeDbParameter[])null);
        }

        /// <summary>
        /// Execute a SnowflakeDbCommand (that returns a resultset) against the database specified in the connection string 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  DataSet ds = ExecuteDataset(connString, CommandType.StoredProcedure, "GetOrders", new SnowflakeDbParameter("@prodid", 24));
        /// </remarks>
        /// <param name="connectionString">a valid connection string for a SnowflakeDbConnection</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <param name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>a dataset containing the resultset generated by the command</returns>
        public static DataSet ExecuteDataset(string connectionString, CommandType commandType, string commandText, params SnowflakeDbParameter[] commandParameters)
        {
            //create & open a SnowflakeDbConnection, and dispose of it after we are done.
            using (SnowflakeDbConnection cn = new SnowflakeDbConnection())
            {
                cn.ConnectionString = connectionString;
                cn.Open();

                //call the overload that takes a connection in place of the connection string
                return ExecuteDataset(cn, commandType, commandText, commandParameters);
            }
        }

        /// <summary>
        /// Execute a stored procedure via a SnowflakeDbCommand (that returns a resultset) against the database specified in 
        /// the connection string using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// </summary>
        /// <remarks>
        /// This method provides no access to output parameters or the stored procedure's return value parameter.
        /// 
        /// e.g.:  
        ///  DataSet ds = ExecuteDataset(connString, "GetOrders", 24, 36);
        /// </remarks>
        /// <param name="connectionString">a valid connection string for a SnowflakeDbConnection</param>
        /// <param name="spName">the name of the stored procedure</param>
        /// <param name="parameterValues">an array of objects to be assigned as the input values of the stored procedure</param>
        /// <returns>a dataset containing the resultset generated by the command</returns>
        public static DataSet ExecuteDataset(string connectionString, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                //pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                SnowflakeDbParameter[] commandParameters = SqlHelperParameterCache.GetSpParameterSet(connectionString, spName);

                //assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, parameterValues);

                //call the overload that takes an array of SnowflakeDbParameters
                return ExecuteDataset(connectionString, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteDataset(connectionString, CommandType.StoredProcedure, spName);
            }
        }

        /// <summary>
        /// Execute a SnowflakeDbCommand (that returns a resultset and takes no parameters) against the provided SnowflakeDbConnection. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  DataSet ds = ExecuteDataset(conn, CommandType.StoredProcedure, "GetOrders");
        /// </remarks>
        /// <param name="connection">a valid SnowflakeDbConnection</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <returns>a dataset containing the resultset generated by the command</returns>
        public static DataSet ExecuteDataset(SnowflakeDbConnection connection, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SnowflakeDbParameters
            return ExecuteDataset(connection, commandType, commandText, (SnowflakeDbParameter[])null);
        }

        /// <summary>
        /// Execute a SnowflakeDbCommand (that returns a resultset) against the specified SnowflakeDbConnection 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  DataSet ds = ExecuteDataset(conn, CommandType.StoredProcedure, "GetOrders", new SnowflakeDbParameter("@prodid", 24));
        /// </remarks>
        /// <param name="connection">a valid SnowflakeDbConnection</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <param name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>a dataset containing the resultset generated by the command</returns>
        public static DataSet ExecuteDataset(SnowflakeDbConnection connection, CommandType commandType, string commandText, params SnowflakeDbParameter[] commandParameters)
        {
            //create a command and prepare it for execution
            System.Data.Common.DbCommand cmd = connection.CreateCommand();
            PrepareCommand(cmd, connection, (SnowflakeDbTransaction)null, commandType, commandText, commandParameters);

            //create the DataAdapter & DataSet
            SnowflakeDbDataAdapter da = new SnowflakeDbDataAdapter(commandText, connection);
            DataSet ds = new DataSet();
            cmd.CommandTimeout = 400;
            //fill the DataSet using default values for DataTable names, etc.
            da.Fill(ds);

            // detach the SnowflakeDbParameters from the command object, so they can be used again.            
            cmd.Parameters.Clear();

            //return the dataset
            return ds;
        }

        /// <summary>
        /// Execute a stored procedure via a SnowflakeDbCommand (that returns a resultset) against the specified SnowflakeDbConnection 
        /// using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// </summary>
        /// <remarks>
        /// This method provides no access to output parameters or the stored procedure's return value parameter.
        /// 
        /// e.g.:  
        ///  DataSet ds = ExecuteDataset(conn, "GetOrders", 24, 36);
        /// </remarks>
        /// <param name="connection">a valid SnowflakeDbConnection</param>
        /// <param name="spName">the name of the stored procedure</param>
        /// <param name="parameterValues">an array of objects to be assigned as the input values of the stored procedure</param>
        /// <returns>a dataset containing the resultset generated by the command</returns>
        public static DataSet ExecuteDataset(SnowflakeDbConnection connection, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                //pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                SnowflakeDbParameter[] commandParameters = SqlHelperParameterCache.GetSpParameterSet(connection.ConnectionString, spName);

                //assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, parameterValues);

                //call the overload that takes an array of SnowflakeDbParameters
                return ExecuteDataset(connection, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteDataset(connection, CommandType.StoredProcedure, spName);
            }
        }

        /// <summary>
        /// Execute a SnowflakeDbCommand (that returns a resultset and takes no parameters) against the provided SnowflakeDbTransaction. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  DataSet ds = ExecuteDataset(trans, CommandType.StoredProcedure, "GetOrders");
        /// </remarks>
        /// <param name="transaction">a valid SnowflakeDbTransaction</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <returns>a dataset containing the resultset generated by the command</returns>
        public static DataSet ExecuteDataset(SnowflakeDbTransaction transaction, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SnowflakeDbParameters
            return ExecuteDataset(transaction, commandType, commandText, (SnowflakeDbParameter[])null);
        }

        /// <summary>
        /// Execute a SnowflakeDbCommand (that returns a resultset) against the specified SnowflakeDbTransaction
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  DataSet ds = ExecuteDataset(trans, CommandType.StoredProcedure, "GetOrders", new SnowflakeDbParameter("@prodid", 24));
        /// </remarks>
        /// <param name="transaction">a valid SnowflakeDbTransaction</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <param name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>a dataset containing the resultset generated by the command</returns>
        public static DataSet ExecuteDataset(SnowflakeDbTransaction transaction, CommandType commandType, string commandText, params SnowflakeDbParameter[] commandParameters)
        {
            //create a command and prepare it for execution
            SnowflakeDbCommand cmd = (SnowflakeDbCommand)transaction.Connection.CreateCommand();
            PrepareCommand(cmd, transaction.Connection, transaction, commandType, commandText, commandParameters);

            //create the DataAdapter & DataSet
            SnowflakeDbDataAdapter da = new SnowflakeDbDataAdapter(commandText, (SnowflakeDbConnection)transaction.Connection);
            DataSet ds = new DataSet();

            //fill the DataSet using default values for DataTable names, etc.
            da.Fill(ds);

            // detach the SnowflakeDbParameters from the command object, so they can be used again.
            cmd.Parameters.Clear();

            //return the dataset
            return ds;
        }

        /// <summary>
        /// Execute a stored procedure via a SnowflakeDbCommand (that returns a resultset) against the specified 
        /// SnowflakeDbTransaction using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// </summary>
        /// <remarks>
        /// This method provides no access to output parameters or the stored procedure's return value parameter.
        /// 
        /// e.g.:  
        ///  DataSet ds = ExecuteDataset(trans, "GetOrders", 24, 36);
        /// </remarks>
        /// <param name="transaction">a valid SnowflakeDbTransaction</param>
        /// <param name="spName">the name of the stored procedure</param>
        /// <param name="parameterValues">an array of objects to be assigned as the input values of the stored procedure</param>
        /// <returns>a dataset containing the resultset generated by the command</returns>
        public static DataSet ExecuteDataset(SnowflakeDbTransaction transaction, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                //pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                SnowflakeDbParameter[] commandParameters = SqlHelperParameterCache.GetSpParameterSet(transaction.Connection.ConnectionString, spName);

                //assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, parameterValues);

                //call the overload that takes an array of SnowflakeDbParameters
                return ExecuteDataset(transaction, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteDataset(transaction, CommandType.StoredProcedure, spName);
            }
        }

        #endregion ExecuteDataSet

        #region ExecuteReader

        /// <summary>
        /// this enum is used to indicate whether the connection was provided by the caller, or created by SqlHelper, so that
        /// we can set the appropriate CommandBehavior when calling ExecuteReader()
        /// </summary>
        private enum SnowflakeDbConnectionOwnership
        {
            /// <summary>Connection is owned and managed by SqlHelper</summary>
            Internal,
            /// <summary>Connection is owned and managed by the caller</summary>
            External
        }

        /// <summary>
        /// Create and prepare a SnowflakeDbCommand, and call ExecuteReader with the appropriate CommandBehavior.
        /// </summary>
        /// <remarks>
        /// If we created and opened the connection, we want the connection to be closed when the DataReader is closed.
        /// 
        /// If the caller provided the connection, we want to leave it to them to manage.
        /// </remarks>
        /// <param name="connection">a valid SnowflakeDbConnection, on which to execute this command</param>
        /// <param name="transaction">a valid SnowflakeDbTransaction, or 'null'</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <param name="commandParameters">an array of SnowflakeDbParameters to be associated with the command or 'null' if no parameters are required</param>
        /// <param name="connectionOwnership">indicates whether the connection parameter was provided by the caller, or created by SqlHelper</param>
        /// <returns>SqlDataReader containing the results of the command</returns>
        private static SnowflakeDbDataReader ExecuteReader(SnowflakeDbConnection connection, SnowflakeDbTransaction transaction, CommandType commandType, string commandText, SnowflakeDbParameter[] commandParameters, SnowflakeDbConnectionOwnership connectionOwnership)
        {
            //create a command and prepare it for execution
            SnowflakeDbCommand cmd = (SnowflakeDbCommand)connection.CreateCommand();
            PrepareCommand(cmd, connection, transaction, commandType, commandText, commandParameters);

            //create a reader
            SnowflakeDbDataReader dr;

            // call ExecuteReader with the appropriate CommandBehavior
            if (connectionOwnership == SnowflakeDbConnectionOwnership.External)
            {
                dr = (SnowflakeDbDataReader)cmd.ExecuteReader();
            }
            else
            {
                dr = (SnowflakeDbDataReader)cmd.ExecuteReader(CommandBehavior.CloseConnection);
            }

            // detach the SnowflakeDbParameters from the command object, so they can be used again.
            cmd.Parameters.Clear();

            return dr;
        }

        /// <summary>
        /// Execute a SnowflakeDbCommand (that returns a resultset and takes no parameters) against the database specified in 
        /// the connection string. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  SqlDataReader dr = ExecuteReader(connString, CommandType.StoredProcedure, "GetOrders");
        /// </remarks>
        /// <param name="connectionString">a valid connection string for a SnowflakeDbConnection</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <returns>a SqlDataReader containing the resultset generated by the command</returns>
        public static SnowflakeDbDataReader ExecuteReader(string connectionString, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SnowflakeDbParameters
            return ExecuteReader(connectionString, commandType, commandText, (SnowflakeDbParameter[])null);
        }

        /// <summary>
        /// Execute a SnowflakeDbCommand (that returns a resultset) against the database specified in the connection string 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  SqlDataReader dr = ExecuteReader(connString, CommandType.StoredProcedure, "GetOrders", new SnowflakeDbParameter("@prodid", 24));
        /// </remarks>
        /// <param name="connectionString">a valid connection string for a SnowflakeDbConnection</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <param name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>a SqlDataReader containing the resultset generated by the command</returns>
        public static SnowflakeDbDataReader ExecuteReader(string connectionString, CommandType commandType, string commandText, params SnowflakeDbParameter[] commandParameters)
        {
            //create & open a SnowflakeDbConnection
            SnowflakeDbConnection cn = new SnowflakeDbConnection();
            cn.ConnectionString = connectionString;
            cn.Open();

            try
            {
                //call the private overload that takes an internally owned connection in place of the connection string
                return ExecuteReader(cn, null, commandType, commandText, commandParameters, SnowflakeDbConnectionOwnership.Internal);
            }
            catch
            {
                //if we fail to return the SqlDatReader, we need to close the connection ourselves
                cn.Close();
                throw;
            }
        }

        /// <summary>
        /// Execute a stored procedure via a SnowflakeDbCommand (that returns a resultset) against the database specified in 
        /// the connection string using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// </summary>
        /// <remarks>
        /// This method provides no access to output parameters or the stored procedure's return value parameter.
        /// 
        /// e.g.:  
        ///  SqlDataReader dr = ExecuteReader(connString, "GetOrders", 24, 36);
        /// </remarks>
        /// <param name="connectionString">a valid connection string for a SnowflakeDbConnection</param>
        /// <param name="spName">the name of the stored procedure</param>
        /// <param name="parameterValues">an array of objects to be assigned as the input values of the stored procedure</param>
        /// <returns>a SqlDataReader containing the resultset generated by the command</returns>
        public static SnowflakeDbDataReader ExecuteReader(string connectionString, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                //pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                SnowflakeDbParameter[] commandParameters = SqlHelperParameterCache.GetSpParameterSet(connectionString, spName);

                //assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, parameterValues);

                //call the overload that takes an array of SnowflakeDbParameters
                return ExecuteReader(connectionString, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteReader(connectionString, CommandType.StoredProcedure, spName);
            }
        }

        /// <summary>
        /// Execute a SnowflakeDbCommand (that returns a resultset and takes no parameters) against the provided SnowflakeDbConnection. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  SqlDataReader dr = ExecuteReader(conn, CommandType.StoredProcedure, "GetOrders");
        /// </remarks>
        /// <param name="connection">a valid SnowflakeDbConnection</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <returns>a SqlDataReader containing the resultset generated by the command</returns>
        public static SnowflakeDbDataReader ExecuteReader(SnowflakeDbConnection connection, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SnowflakeDbParameters
            return ExecuteReader(connection, commandType, commandText, (SnowflakeDbParameter[])null);
        }

        /// <summary>
        /// Execute a SnowflakeDbCommand (that returns a resultset) against the specified SnowflakeDbConnection 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  SqlDataReader dr = ExecuteReader(conn, CommandType.StoredProcedure, "GetOrders", new SnowflakeDbParameter("@prodid", 24));
        /// </remarks>
        /// <param name="connection">a valid SnowflakeDbConnection</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <param name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>a SqlDataReader containing the resultset generated by the command</returns>
        public static SnowflakeDbDataReader ExecuteReader(SnowflakeDbConnection connection, CommandType commandType, string commandText, params SnowflakeDbParameter[] commandParameters)
        {
            //pass through the call to the private overload using a null transaction value and an externally owned connection
            return ExecuteReader(connection, (SnowflakeDbTransaction)null, commandType, commandText, commandParameters, SnowflakeDbConnectionOwnership.External);
        }

        /// <summary>
        /// Execute a stored procedure via a SnowflakeDbCommand (that returns a resultset) against the specified SnowflakeDbConnection 
        /// using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// </summary>
        /// <remarks>
        /// This method provides no access to output parameters or the stored procedure's return value parameter.
        /// 
        /// e.g.:  
        ///  SqlDataReader dr = ExecuteReader(conn, "GetOrders", 24, 36);
        /// </remarks>
        /// <param name="connection">a valid SnowflakeDbConnection</param>
        /// <param name="spName">the name of the stored procedure</param>
        /// <param name="parameterValues">an array of objects to be assigned as the input values of the stored procedure</param>
        /// <returns>a SqlDataReader containing the resultset generated by the command</returns>
        public static SnowflakeDbDataReader ExecuteReader(SnowflakeDbConnection connection, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                SnowflakeDbParameter[] commandParameters = SqlHelperParameterCache.GetSpParameterSet(connection.ConnectionString, spName);

                AssignParameterValues(commandParameters, parameterValues);

                return ExecuteReader(connection, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteReader(connection, CommandType.StoredProcedure, spName);
            }
        }

        /// <summary>
        /// Execute a SnowflakeDbCommand (that returns a resultset and takes no parameters) against the provided SnowflakeDbTransaction. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  SqlDataReader dr = ExecuteReader(trans, CommandType.StoredProcedure, "GetOrders");
        /// </remarks>
        /// <param name="transaction">a valid SnowflakeDbTransaction</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <returns>a SqlDataReader containing the resultset generated by the command</returns>
        public static SnowflakeDbDataReader ExecuteReader(SnowflakeDbTransaction transaction, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SnowflakeDbParameters
            return ExecuteReader(transaction, commandType, commandText, (SnowflakeDbParameter[])null);
        }

        /// <summary>
        /// Execute a SnowflakeDbCommand (that returns a resultset) against the specified SnowflakeDbTransaction
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///   SqlDataReader dr = ExecuteReader(trans, CommandType.StoredProcedure, "GetOrders", new SnowflakeDbParameter("@prodid", 24));
        /// </remarks>
        /// <param name="transaction">a valid SnowflakeDbTransaction</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <param name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>a SqlDataReader containing the resultset generated by the command</returns>
        public static SnowflakeDbDataReader ExecuteReader(SnowflakeDbTransaction transaction, CommandType commandType, string commandText, params SnowflakeDbParameter[] commandParameters)
        {
            //pass through to private overload, indicating that the connection is owned by the caller
            return ExecuteReader((SnowflakeDbConnection)transaction.Connection, transaction, commandType, commandText, commandParameters, SnowflakeDbConnectionOwnership.External);
        }

        /// <summary>
        /// Execute a stored procedure via a SnowflakeDbCommand (that returns a resultset) against the specified
        /// SnowflakeDbTransaction using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// </summary>
        /// <remarks>
        /// This method provides no access to output parameters or the stored procedure's return value parameter.
        /// 
        /// e.g.:  
        ///  SqlDataReader dr = ExecuteReader(trans, "GetOrders", 24, 36);
        /// </remarks>
        /// <param name="transaction">a valid SnowflakeDbTransaction</param>
        /// <param name="spName">the name of the stored procedure</param>
        /// <param name="parameterValues">an array of objects to be assigned as the input values of the stored procedure</param>
        /// <returns>a SqlDataReader containing the resultset generated by the command</returns>
        public static SnowflakeDbDataReader ExecuteReader(SnowflakeDbTransaction transaction, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                SnowflakeDbParameter[] commandParameters = SqlHelperParameterCache.GetSpParameterSet(transaction.Connection.ConnectionString, spName);

                AssignParameterValues(commandParameters, parameterValues);

                return ExecuteReader(transaction, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteReader(transaction, CommandType.StoredProcedure, spName);
            }
        }

        #endregion ExecuteReader

        #region ExecuteScalar

        /// <summary>
        /// Execute a SnowflakeDbCommand (that returns a 1x1 resultset and takes no parameters) against the database specified in 
        /// the connection string. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int orderCount = (int)ExecuteScalar(connString, CommandType.StoredProcedure, "GetOrderCount");
        /// </remarks>
        /// <param name="connectionString">a valid connection string for a SnowflakeDbConnection</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <returns>an object containing the value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(string connectionString, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SnowflakeDbParameters
            return ExecuteScalar(connectionString, commandType, commandText, (SnowflakeDbParameter[])null);
        }

        /// <summary>
        /// Execute a SnowflakeDbCommand (that returns a 1x1 resultset) against the database specified in the connection string 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int orderCount = (int)ExecuteScalar(connString, CommandType.StoredProcedure, "GetOrderCount", new SnowflakeDbParameter("@prodid", 24));
        /// </remarks>
        /// <param name="connectionString">a valid connection string for a SnowflakeDbConnection</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <param name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>an object containing the value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(string connectionString, CommandType commandType, string commandText, params SnowflakeDbParameter[] commandParameters)
        {
            //create & open a SnowflakeDbConnection, and dispose of it after we are done.
            using (SnowflakeDbConnection cn = new SnowflakeDbConnection())
            {
                cn.ConnectionString = connectionString;
                cn.Open();

                //call the overload that takes a connection in place of the connection string
                return ExecuteScalar(cn, commandType, commandText, commandParameters);
            }
        }

        /// <summary>
        /// Execute a stored procedure via a SnowflakeDbCommand (that returns a 1x1 resultset) against the database specified in 
        /// the connection string using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// </summary>
        /// <remarks>
        /// This method provides no access to output parameters or the stored procedure's return value parameter.
        /// 
        /// e.g.:  
        ///  int orderCount = (int)ExecuteScalar(connString, "GetOrderCount", 24, 36);
        /// </remarks>
        /// <param name="connectionString">a valid connection string for a SnowflakeDbConnection</param>
        /// <param name="spName">the name of the stored procedure</param>
        /// <param name="parameterValues">an array of objects to be assigned as the input values of the stored procedure</param>
        /// <returns>an object containing the value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(string connectionString, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                //pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                SnowflakeDbParameter[] commandParameters = SqlHelperParameterCache.GetSpParameterSet(connectionString, spName);

                //assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, parameterValues);

                //call the overload that takes an array of SnowflakeDbParameters
                return ExecuteScalar(connectionString, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteScalar(connectionString, CommandType.StoredProcedure, spName);
            }
        }

        /// <summary>
        /// Execute a SnowflakeDbCommand (that returns a 1x1 resultset and takes no parameters) against the provided SnowflakeDbConnection. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int orderCount = (int)ExecuteScalar(conn, CommandType.StoredProcedure, "GetOrderCount");
        /// </remarks>
        /// <param name="connection">a valid SnowflakeDbConnection</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <returns>an object containing the value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(SnowflakeDbConnection connection, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SnowflakeDbParameters
            return ExecuteScalar(connection, commandType, commandText, (SnowflakeDbParameter[])null);
        }

        /// <summary>
        /// Execute a SnowflakeDbCommand (that returns a 1x1 resultset) against the specified SnowflakeDbConnection 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int orderCount = (int)ExecuteScalar(conn, CommandType.StoredProcedure, "GetOrderCount", new SnowflakeDbParameter("@prodid", 24));
        /// </remarks>
        /// <param name="connection">a valid SnowflakeDbConnection</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <param name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>an object containing the value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(SnowflakeDbConnection connection, CommandType commandType, string commandText, params SnowflakeDbParameter[] commandParameters)
        {
            //create a command and prepare it for execution
            SnowflakeDbCommand cmd = (SnowflakeDbCommand)connection.CreateCommand();
            PrepareCommand(cmd, connection, (SnowflakeDbTransaction)null, commandType, commandText, commandParameters);

            //execute the command & return the results
            object retval = cmd.ExecuteScalar();

            // detach the SnowflakeDbParameters from the command object, so they can be used again.
            cmd.Parameters.Clear();
            return retval;

        }

        /// <summary>
        /// Execute a stored procedure via a SnowflakeDbCommand (that returns a 1x1 resultset) against the specified SnowflakeDbConnection 
        /// using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// </summary>
        /// <remarks>
        /// This method provides no access to output parameters or the stored procedure's return value parameter.
        /// 
        /// e.g.:  
        ///  int orderCount = (int)ExecuteScalar(conn, "GetOrderCount", 24, 36);
        /// </remarks>
        /// <param name="connection">a valid SnowflakeDbConnection</param>
        /// <param name="spName">the name of the stored procedure</param>
        /// <param name="parameterValues">an array of objects to be assigned as the input values of the stored procedure</param>
        /// <returns>an object containing the value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(SnowflakeDbConnection connection, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                //pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                SnowflakeDbParameter[] commandParameters = SqlHelperParameterCache.GetSpParameterSet(connection.ConnectionString, spName);

                //assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, parameterValues);

                //call the overload that takes an array of SnowflakeDbParameters
                return ExecuteScalar(connection, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteScalar(connection, CommandType.StoredProcedure, spName);
            }
        }

        /// <summary>
        /// Execute a SnowflakeDbCommand (that returns a 1x1 resultset and takes no parameters) against the provided SnowflakeDbTransaction. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int orderCount = (int)ExecuteScalar(trans, CommandType.StoredProcedure, "GetOrderCount");
        /// </remarks>
        /// <param name="transaction">a valid SnowflakeDbTransaction</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <returns>an object containing the value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(SnowflakeDbTransaction transaction, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SnowflakeDbParameters
            return ExecuteScalar(transaction, commandType, commandText, (SnowflakeDbParameter[])null);
        }

        /// <summary>
        /// Execute a SnowflakeDbCommand (that returns a 1x1 resultset) against the specified SnowflakeDbTransaction
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int orderCount = (int)ExecuteScalar(trans, CommandType.StoredProcedure, "GetOrderCount", new SnowflakeDbParameter("@prodid", 24));
        /// </remarks>
        /// <param name="transaction">a valid SnowflakeDbTransaction</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <param name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>an object containing the value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(SnowflakeDbTransaction transaction, CommandType commandType, string commandText, params SnowflakeDbParameter[] commandParameters)
        {
            //create a command and prepare it for execution
            SnowflakeDbCommand cmd = (SnowflakeDbCommand)transaction.Connection.CreateCommand();
            PrepareCommand(cmd, transaction.Connection, transaction, commandType, commandText, commandParameters);

            //execute the command & return the results
            object retval = cmd.ExecuteScalar();

            // detach the SnowflakeDbParameters from the command object, so they can be used again.
            cmd.Parameters.Clear();
            return retval;
        }

        /// <summary>
        /// Execute a stored procedure via a SnowflakeDbCommand (that returns a 1x1 resultset) against the specified
        /// SnowflakeDbTransaction using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// </summary>
        /// <remarks>
        /// This method provides no access to output parameters or the stored procedure's return value parameter.
        /// 
        /// e.g.:  
        ///  int orderCount = (int)ExecuteScalar(trans, "GetOrderCount", 24, 36);
        /// </remarks>
        /// <param name="transaction">a valid SnowflakeDbTransaction</param>
        /// <param name="spName">the name of the stored procedure</param>
        /// <param name="parameterValues">an array of objects to be assigned as the input values of the stored procedure</param>
        /// <returns>an object containing the value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(SnowflakeDbTransaction transaction, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                //pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                SnowflakeDbParameter[] commandParameters = SqlHelperParameterCache.GetSpParameterSet(transaction.Connection.ConnectionString, spName);

                //assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, parameterValues);

                //call the overload that takes an array of SnowflakeDbParameters
                return ExecuteScalar(transaction, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteScalar(transaction, CommandType.StoredProcedure, spName);
            }
        }

        #endregion ExecuteScalar

        #region ExecuteXmlReader

        /// <summary>
        /// Execute a SnowflakeDbCommand (that returns a resultset and takes no parameters) against the provided SnowflakeDbConnection. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  XmlReader r = ExecuteXmlReader(conn, CommandType.StoredProcedure, "GetOrders");
        /// </remarks>
        /// <param name="connection">a valid SnowflakeDbConnection</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command using "FOR XML AUTO"</param>
        /// <returns>an XmlReader containing the resultset generated by the command</returns>
        public static XmlReader ExecuteXmlReader(SnowflakeDbConnection connection, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SnowflakeDbParameters
            return ExecuteXmlReader(connection, commandType, commandText, (SnowflakeDbParameter[])null);
        }

        /// <summary>
        /// Execute a SnowflakeDbCommand (that returns a resultset) against the specified SnowflakeDbConnection 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  XmlReader r = ExecuteXmlReader(conn, CommandType.StoredProcedure, "GetOrders", new SnowflakeDbParameter("@prodid", 24));
        /// </remarks>
        /// <param name="connection">a valid SnowflakeDbConnection</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command using "FOR XML AUTO"</param>
        /// <param name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>an XmlReader containing the resultset generated by the command</returns>
        public static XmlReader ExecuteXmlReader(SnowflakeDbConnection connection, CommandType commandType, string commandText, params SnowflakeDbParameter[] commandParameters)
        {
            //create a command and prepare it for execution
            SnowflakeDbCommand cmd = (SnowflakeDbCommand)connection.CreateCommand();
            PrepareCommand(cmd, connection, (SnowflakeDbTransaction)null, commandType, commandText, commandParameters);

            //create the DataAdapter & DataSet

            throw new Exception("ExecuteXmlReader not implemented");
            // XmlReader retval = cmd.ExecuteXmlReader();

            // detach the SnowflakeDbParameters from the command object, so they can be used again.
            //cmd.Parameters.Clear();
            //return retval;

        }

        /// <summary>
        /// Execute a stored procedure via a SnowflakeDbCommand (that returns a resultset) against the specified SnowflakeDbConnection 
        /// using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// </summary>
        /// <remarks>
        /// This method provides no access to output parameters or the stored procedure's return value parameter.
        /// 
        /// e.g.:  
        ///  XmlReader r = ExecuteXmlReader(conn, "GetOrders", 24, 36);
        /// </remarks>
        /// <param name="connection">a valid SnowflakeDbConnection</param>
        /// <param name="spName">the name of the stored procedure using "FOR XML AUTO"</param>
        /// <param name="parameterValues">an array of objects to be assigned as the input values of the stored procedure</param>
        /// <returns>an XmlReader containing the resultset generated by the command</returns>
        public static XmlReader ExecuteXmlReader(SnowflakeDbConnection connection, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                //pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                SnowflakeDbParameter[] commandParameters = SqlHelperParameterCache.GetSpParameterSet(connection.ConnectionString, spName);

                //assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, parameterValues);

                //call the overload that takes an array of SnowflakeDbParameters
                return ExecuteXmlReader(connection, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteXmlReader(connection, CommandType.StoredProcedure, spName);
            }
        }

        /// <summary>
        /// Execute a SnowflakeDbCommand (that returns a resultset and takes no parameters) against the provided SnowflakeDbTransaction. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  XmlReader r = ExecuteXmlReader(trans, CommandType.StoredProcedure, "GetOrders");
        /// </remarks>
        /// <param name="transaction">a valid SnowflakeDbTransaction</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command using "FOR XML AUTO"</param>
        /// <returns>an XmlReader containing the resultset generated by the command</returns>
        public static XmlReader ExecuteXmlReader(SnowflakeDbTransaction transaction, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SnowflakeDbParameters
            return ExecuteXmlReader(transaction, commandType, commandText, (SnowflakeDbParameter[])null);
        }

        /// <summary>
        /// Execute a SnowflakeDbCommand (that returns a resultset) against the specified SnowflakeDbTransaction
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  XmlReader r = ExecuteXmlReader(trans, CommandType.StoredProcedure, "GetOrders", new SnowflakeDbParameter("@prodid", 24));
        /// </remarks>
        /// <param name="transaction">a valid SnowflakeDbTransaction</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command using "FOR XML AUTO"</param>
        /// <param name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>an XmlReader containing the resultset generated by the command</returns>
        public static XmlReader ExecuteXmlReader(SnowflakeDbTransaction transaction, CommandType commandType, string commandText, params SnowflakeDbParameter[] commandParameters)
        {
            //create a command and prepare it for execution
            SnowflakeDbCommand cmd = (SnowflakeDbCommand)transaction.Connection.CreateCommand();
            PrepareCommand(cmd, transaction.Connection, transaction, commandType, commandText, commandParameters);

            //create the DataAdapter & DataSet

            // XmlReader retval = cmd.ExecuteXmlReader();
            throw new Exception("ExecuteXmlReader not implemented");


            // detach the SnowflakeDbParameters from the command object, so they can be used again.
            //cmd.Parameters.Clear();
            //return retval;
        }

        /// <summary>
        /// Execute a stored procedure via a SnowflakeDbCommand (that returns a resultset) against the specified 
        /// SnowflakeDbTransaction using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// </summary>
        /// <remarks>
        /// This method provides no access to output parameters or the stored procedure's return value parameter.
        /// 
        /// e.g.:  
        ///  XmlReader r = ExecuteXmlReader(trans, "GetOrders", 24, 36);
        /// </remarks>
        /// <param name="transaction">a valid SnowflakeDbTransaction</param>
        /// <param name="spName">the name of the stored procedure</param>
        /// <param name="parameterValues">an array of objects to be assigned as the input values of the stored procedure</param>
        /// <returns>a dataset containing the resultset generated by the command</returns>
        public static XmlReader ExecuteXmlReader(SnowflakeDbTransaction transaction, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                //pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                SnowflakeDbParameter[] commandParameters = SqlHelperParameterCache.GetSpParameterSet(transaction.Connection.ConnectionString, spName);

                //assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, parameterValues);

                //call the overload that takes an array of SnowflakeDbParameters
                return ExecuteXmlReader(transaction, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteXmlReader(transaction, CommandType.StoredProcedure, spName);
            }
        }


        #endregion ExecuteXmlReader
    }

    /// <summary>
    /// SqlHelperParameterCache provides functions to leverage a static cache of procedure parameters, and the
    /// ability to discover parameters for stored procedures at run-time.
    /// </summary>
    public sealed class SqlHelperParameterCache
    {
        #region private methods, variables, and constructors

        //Since this class provides only static methods, make the default constructor private to prevent 
        //instances from being created with "new SqlHelperParameterCache()".
        private SqlHelperParameterCache() { }

        private static Hashtable paramCache = Hashtable.Synchronized(new Hashtable());

        /// <summary>
        /// resolve at run time the appropriate set of SnowflakeDbParameters for a stored procedure
        /// </summary>
        /// <param name="connectionString">a valid connection string for a SnowflakeDbConnection</param>
        /// <param name="spName">the name of the stored procedure</param>
        /// <param name="includeReturnValueParameter">whether or not to include their return value parameter</param>
        /// <returns></returns>
        private static SnowflakeDbParameter[] DiscoverSpParameterSet(string connectionString, string spName, bool includeReturnValueParameter)
        {
            //using (SnowflakeDbConnection cn = new SnowflakeDbConnection())
            //{
            //    cn.ConnectionString = connectionString;
            //    using (SnowflakeDbCommand cmd = new SnowflakeDbCommand(spName, cn))
            using (SqlConnection cn = new SqlConnection(connectionString))
            using (SqlCommand cmd = new SqlCommand(spName, cn))
            {
                cn.Open();
                cmd.CommandType = CommandType.StoredProcedure;

                SqlCommandBuilder.DeriveParameters(cmd);

                if (!includeReturnValueParameter)
                {
                    cmd.Parameters.RemoveAt(0);
                }

                SnowflakeDbParameter[] discoveredParameters = new SnowflakeDbParameter[cmd.Parameters.Count]; ;

                cmd.Parameters.CopyTo(discoveredParameters, 0);

                return discoveredParameters;
            }
            //    }
        }

        //deep copy of cached SnowflakeDbParameter array
        private static SnowflakeDbParameter[] CloneParameters(SnowflakeDbParameter[] originalParameters)
        {
            SnowflakeDbParameter[] clonedParameters = new SnowflakeDbParameter[originalParameters.Length];

            for (int i = 0, j = originalParameters.Length; i < j; i++)
            {
                clonedParameters[i] = (SnowflakeDbParameter)((ICloneable)originalParameters[i]).Clone();
            }

            return clonedParameters;
        }

        #endregion private methods, variables, and constructors

        #region caching functions

        /// <summary>
        /// add parameter array to the cache
        /// </summary>
        /// <param name="connectionString">a valid connection string for a SnowflakeDbConnection</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <param name="commandParameters">an array of SqlParamters to be cached</param>
        public static void CacheParameterSet(string connectionString, string commandText, params SnowflakeDbParameter[] commandParameters)
        {
            string hashKey = connectionString + ":" + commandText;

            paramCache[hashKey] = commandParameters;
        }

        /// <summary>
        /// retrieve a parameter array from the cache
        /// </summary>
        /// <param name="connectionString">a valid connection string for a SnowflakeDbConnection</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <returns>an array of SqlParamters</returns>
        public static SnowflakeDbParameter[] GetCachedParameterSet(string connectionString, string commandText)
        {
            string hashKey = connectionString + ":" + commandText;

            SnowflakeDbParameter[] cachedParameters = (SnowflakeDbParameter[])paramCache[hashKey];

            if (cachedParameters == null)
            {
                return null;
            }
            else
            {
                return CloneParameters(cachedParameters);
            }
        }

        #endregion caching functions

        #region Parameter Discovery Functions

        /// <summary>
        /// Retrieves the set of SnowflakeDbParameters appropriate for the stored procedure
        /// </summary>
        /// <remarks>
        /// This method will query the database for this information, and then store it in a cache for future requests.
        /// </remarks>
        /// <param name="connectionString">a valid connection string for a SnowflakeDbConnection</param>
        /// <param name="spName">the name of the stored procedure</param>
        /// <returns>an array of SnowflakeDbParameters</returns>
        public static SnowflakeDbParameter[] GetSpParameterSet(string connectionString, string spName)
        {
            return GetSpParameterSet(connectionString, spName, false);
        }

        /// <summary>
        /// Retrieves the set of SnowflakeDbParameters appropriate for the stored procedure
        /// </summary>
        /// <remarks>
        /// This method will query the database for this information, and then store it in a cache for future requests.
        /// </remarks>
        /// <param name="connectionString">a valid connection string for a SnowflakeDbConnection</param>
        /// <param name="spName">the name of the stored procedure</param>
        /// <param name="includeReturnValueParameter">a bool value indicating whether the return value parameter should be included in the results</param>
        /// <returns>an array of SnowflakeDbParameters</returns>
        public static SnowflakeDbParameter[] GetSpParameterSet(string connectionString, string spName, bool includeReturnValueParameter)
        {
            string hashKey = connectionString + ":" + spName + (includeReturnValueParameter ? ":include ReturnValue Parameter" : "");

            SnowflakeDbParameter[] cachedParameters;

            cachedParameters = (SnowflakeDbParameter[])paramCache[hashKey];

            if (cachedParameters == null)
            {
                cachedParameters = (SnowflakeDbParameter[])(paramCache[hashKey] = DiscoverSpParameterSet(connectionString, spName, includeReturnValueParameter));
            }

            return CloneParameters(cachedParameters);
        }

        #endregion Parameter Discovery Functions

    }
}