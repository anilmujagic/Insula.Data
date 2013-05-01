using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Globalization;
using Insula.Common;
using System.Data.Common;
using System.Data.SqlClient;

namespace Insula.Data.Orm
{
    public class Database : IDisposable
    {
        public Database(DatabaseType databaseType, string connectionString)
        {
            if (!Enum.IsDefined(databaseType.GetType(), databaseType))
                throw new ArgumentException("Unknown DatabaseType.", "databaseType");
            if (connectionString.IsNullOrWhiteSpace())
                throw new ArgumentException("Connection string must not be empty.", "connectionString");

            this.DatabaseType = databaseType;
            _connectionString = connectionString;

            this.InitializeConnection();
        }

        public readonly DatabaseType DatabaseType { get; private set; }
        private readonly string _connectionString;
        private DbConnection _connection;


        #region Connection

        private void InitializeConnection()
        {
            if (_connection == null)
            {
                switch (this.DatabaseType)
                {
                    case DatabaseType.SqlServer:
                        _connection = new SqlConnection(_connectionString);
                        break;
                }
            }
        }

        internal void OpenConnection()
        {
            this.InitializeConnection();

            switch (_connection.State)
            {
                case System.Data.ConnectionState.Broken:
                    _connection.Close();
                    _connection.Open();
                    break;
                case System.Data.ConnectionState.Closed:
                    _connection.Open();
                    break;
            }
        }

        internal void CloseConnection()
        {
            if (_connection != null && _connection.State != System.Data.ConnectionState.Closed)
                _connection.Close();
        }

        #endregion


        #region Commands

        internal DbCommand CreateCommand()
        {
            this.InitializeConnection();
            return _connection.CreateCommand();
        }

        internal DbCommand CreateCommand(string sql, params object[] sqlParameterValues)
        {
            var command = this.CreateCommand();

            command.CommandText = sql;

            if (!sqlParameterValues.IsNullOrEmpty())
            {
                for (int i = 0; i < sqlParameterValues.Length; i++)
                {
                    DbParameter parameter = null;

                    switch (this.DatabaseType)
                    {
                        case DatabaseType.SqlServer:
                            parameter = new SqlParameter("@" + i.ToString(CultureInfo.InvariantCulture), sqlParameterValues[i]);
                            break;
                    }

                    command.Parameters.Add(parameter);
                }
            }

            return command;
        }

        public object ExecuteScalar(string sql, params object[] parameters)
        {
            object result = null;

            using (var command = this.CreateCommand(sql, parameters))
            {
                this.OpenConnection();
                try
                {
                    result = command.ExecuteScalar();
                }
                catch (Exception ex)
                {
                    this.OnException(ex, command.CommandText);
                    throw;
                }
                finally
                {
                    this.CloseConnection();
                }
            }

            return result;
        }

        public int ExecuteNonQuery(string sql, params object[] parameters)
        {
            int result = 0;

            using (var command = this.CreateCommand(sql, parameters))
            {
                this.OpenConnection();
                try
                {
                    result = command.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    this.OnException(ex, command.CommandText);
                    throw;
                }
                finally
                {
                    this.CloseConnection();
                }
            }

            return result;
        }

        #endregion


        public virtual void OnException(Exception exception, string sql)
        {
            var message = string.Join(Environment.NewLine, exception.GetExceptionTreeAsFlatList());
            System.Diagnostics.Debug.WriteLine(message);
            System.Diagnostics.Debug.WriteLine(sql);
        }


        #region IDisposable Members

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (_connection != null)
                {
                    _connection.Dispose();
                    _connection = null;
                }
            }
        }

        #endregion
    }
}
