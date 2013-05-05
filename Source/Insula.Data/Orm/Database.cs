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
        public Database(DatabaseEngine databaseType, string connectionString)
        {
            if (!Enum.IsDefined(databaseType.GetType(), databaseType))
                throw new ArgumentException("Unknown DatabaseType.", "databaseType");
            if (connectionString.IsNullOrWhiteSpace())
                throw new ArgumentException("Connection string must not be empty.", "connectionString");

            this.DatabaseType = databaseType;
            _connectionString = connectionString;

            this.InitializeConnection();

            _tableMetadata = new Dictionary<string, TableMetadata>();
            _materializers = new Dictionary<string, object>();
        }

        public DatabaseEngine DatabaseType { get; private set; }
        private readonly string _connectionString;
        private DbConnection _connection;


        #region Connection

        private void InitializeConnection()
        {
            if (_connection == null)
            {
                switch (this.DatabaseType)
                {
                    case DatabaseEngine.SqlServer:
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
                    var parameter = this.CreateParameter(i.ToString(CultureInfo.InvariantCulture), sqlParameterValues[i]);
                    command.Parameters.Add(parameter);
                }
            }

            return command;
        }

        internal DbParameter CreateParameter(string name, object value)
        {
            DbParameter parameter = null;

            switch (this.DatabaseType)
            {
                case DatabaseEngine.SqlServer:
                    parameter = new SqlParameter("@" + name, value);
                    break;
            }

            return parameter;
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


        #region Cached metadata to improve speed

        private readonly Dictionary<string, TableMetadata> _tableMetadata;
        internal TableMetadata GetTableMetadata(Type type)
        {
            if (!_tableMetadata.ContainsKey(type.FullName))
                _tableMetadata.Add(type.FullName, new TableMetadata(type));

            return _tableMetadata[type.FullName];
        }

        private readonly Dictionary<string, object> _materializers;
        internal object GetMaterializer(Type type)
        {
            if (!_materializers.ContainsKey(type.FullName))
            {
                var tableMetadata = this.GetTableMetadata(type);
                var materializerType = typeof(Materializer<>).MakeGenericType(new Type[] { type });
                var materializer = Activator.CreateInstance(materializerType, tableMetadata);
                _materializers.Add(type.FullName, materializer);
            }

            return _materializers[type.FullName];
        }

        #endregion


        #region Query

        public SqlQuery<T> Query<T>(string customSelectStatement = null, params object[] parameters) where T : class, new()
        {
            return new SqlQuery<T>(this, customSelectStatement, parameters);
        }

        #endregion


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
