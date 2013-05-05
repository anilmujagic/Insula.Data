using Insula.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using System.Globalization;
using System.Data.Common;

namespace Insula.Data.Orm
{
    public class Repository<T> where T : class, new()
    {
        public Repository(Database database)
        {
            _database = database;
            _tableMetadata = _database.GetTableMetadata(typeof(T));
        }

        private readonly Database _database;
        private readonly TableMetadata _tableMetadata;


        #region SQL Statements

        private string _insertSql;
        private string InsertSql
        {
            get
            {
                if (_insertSql == null)
                {
                    _insertSql = "INSERT INTO [{0}] ({1}) VALUES ({2})".FormatString(
                        _tableMetadata.Name,
                        string.Join(", ", _tableMetadata.InsertColumns.Select(c => "[{0}]".FormatString(c.Name))),
                        string.Join(", ", _tableMetadata.InsertColumns.Select(c => "@{0}".FormatString(c.Name))));

                    if (_tableMetadata.IdentityColumn != null)
                        _insertSql += "; SELECT SCOPE_IDENTITY();";
                }
                return _insertSql;
            }
        }

        private string _keyWhereClauseForUpdateAndDelete;
        private string KeyWhereClauseForUpdateAndDelete
        {
            get
            {
                if (_keyWhereClauseForUpdateAndDelete == null)
                {
                    _keyWhereClauseForUpdateAndDelete = string.Join(" AND ", _tableMetadata.KeyColumns
                        .Select(c => "[{0}] = @{0}".FormatString(c.Name)));
                }
                return _keyWhereClauseForUpdateAndDelete;
            }
        }

        private string _updateSql;
        private string UpdateSql
        {
            get
            {
                if (_updateSql == null)
                {
                    _updateSql = "UPDATE [{0}] SET {1} WHERE {2}".FormatString(
                        _tableMetadata.Name,
                        string.Join(", ", _tableMetadata.UpdateColumns.Select(c => "[{0}] = @{0}".FormatString(c.Name))),
                        this.KeyWhereClauseForUpdateAndDelete);
                }
                return _updateSql;
            }
        }

        private string _deleteSql;
        private string DeleteSql
        {
            get
            {
                if (_deleteSql == null)
                {
                    _deleteSql = "DELETE FROM [{0}] WHERE {1}".FormatString(
                        _tableMetadata.Name,
                        this.KeyWhereClauseForUpdateAndDelete);
                }
                return _deleteSql;
            }
        }

        private string _deleteByKeySql;
        private string DeleteByKeySql
        {
            get
            {
                if (_deleteByKeySql == null)
                {
                    _deleteByKeySql = "DELETE FROM [{0}] WHERE {1}".FormatString(
                        _tableMetadata.Name,
                        this.KeyQueryWhereClause);
                }
                return _deleteByKeySql;
            }
        }

        private string _keyQueryWhereClause;
        private string KeyQueryWhereClause
        {
            get
            {
                if (_keyQueryWhereClause == null)
                {
                    int index = 0;
                    _keyQueryWhereClause = string.Join(" AND ", _tableMetadata.KeyColumns
                        .Select(c => "[{0}] = @{1}".FormatString(c.Name, index++)));
                }
                return _keyQueryWhereClause;
            }
        }

        #endregion


        public void Insert(T entity)
        {
            if (entity == null)
                throw new ArgumentNullException("entity");

            var parameters = new List<DbParameter>();
            foreach (var c in _tableMetadata.InsertColumns)
            {
                var value = c.PropertyInfo.GetValue(entity, null);
                parameters.Add(_database.CreateParameter(c.Name, value ?? DBNull.Value));
            }

            using (var command = _database.CreateCommand())
            {
                command.CommandText = this.InsertSql;
                command.Parameters.AddRange(parameters.ToArray());

                object newID;

                _database.OpenConnection();
                try
                {
                    newID = command.ExecuteScalar();
                }
                catch (Exception ex)
                {
                    _database.OnException(ex, command.CommandText);
                    throw;
                }
                finally
                {
                    _database.CloseConnection();
                }

                if (_tableMetadata.IdentityColumn != null)
                {
                    if (_tableMetadata.IdentityColumn.Type == typeof(int))
                        _tableMetadata.IdentityColumn.PropertyInfo.SetValue(entity, Convert.ToInt32(newID, CultureInfo.InvariantCulture), null);
                    else if (_tableMetadata.IdentityColumn.Type == typeof(long))
                        _tableMetadata.IdentityColumn.PropertyInfo.SetValue(entity, Convert.ToInt64(newID, CultureInfo.InvariantCulture), null);
                }
            }
        }

        public void Update(T entity)
        {
            if (entity == null)
                throw new ArgumentNullException("entity");

            if (_tableMetadata.KeyColumns.IsNullOrEmpty())
                throw new SqlStatementException("At least one object property must have a [Key] attribute for UPDATE statement to be valid.");

            var parameters = new List<DbParameter>();
            foreach (var c in _tableMetadata.UpdateColumns)
            {
                var value = c.PropertyInfo.GetValue(entity, null);
                parameters.Add(_database.CreateParameter(c.Name, value ?? DBNull.Value));
            }
            foreach (var c in _tableMetadata.KeyColumns)
            {
                var value = c.PropertyInfo.GetValue(entity, null);
                parameters.Add(_database.CreateParameter(c.Name, value ?? DBNull.Value));
            }

            using (var command = _database.CreateCommand())
            {
                command.CommandText = this.UpdateSql;
                command.Parameters.AddRange(parameters.ToArray());

                _database.OpenConnection();
                try
                {
                    command.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    _database.OnException(ex, command.CommandText);
                    throw;
                }
                finally
                {
                    _database.CloseConnection();
                }
            }
        }

        public void Delete(T entity)
        {
            if (entity == null)
                throw new ArgumentNullException("entity");

            if (_tableMetadata.KeyColumns.IsNullOrEmpty())
                throw new SqlStatementException("At least one object property must have a [Key] attribute for DELETE statement to be valid.");

            var parameters = new List<DbParameter>();
            foreach (var c in _tableMetadata.KeyColumns)
            {
                var value = c.PropertyInfo.GetValue(entity, null);
                parameters.Add(_database.CreateParameter(c.Name, value ?? DBNull.Value));
            }

            using (var command = _database.CreateCommand())
            {
                command.CommandText = this.DeleteSql;
                command.Parameters.AddRange(parameters.ToArray());

                _database.OpenConnection();
                try
                {
                    command.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    _database.OnException(ex, command.CommandText);
                    throw;
                }
                finally
                {
                    _database.CloseConnection();
                }
            }
        }

        public void DeleteByKey(params object[] keyValues)
        {
            if (_tableMetadata.KeyColumns.IsNullOrEmpty())
                throw new SqlStatementException("At least one object property must have a [Key] attribute for DELETE statement to be valid.");
            if (keyValues == null)
                throw new ArgumentNullException("keyValues");
            if (keyValues.Length != _tableMetadata.KeyColumns.Count())
                throw new ArgumentOutOfRangeException("keyValues", keyValues.Length, "Number of passed key values must be equal to number of key columns.");

            var parameters = new List<DbParameter>();
            int index = 0;
            foreach (var value in keyValues)
            {
                parameters.Add(_database.CreateParameter(index.ToString(CultureInfo.InvariantCulture), value ?? DBNull.Value));
                index++;
            }

            using (var command = _database.CreateCommand())
            {
                command.CommandText = this.DeleteByKeySql;
                command.Parameters.AddRange(parameters.ToArray());

                _database.OpenConnection();
                try
                {
                    command.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    _database.OnException(ex, command.CommandText);
                    throw;
                }
                finally
                {
                    _database.CloseConnection();
                }
            }
        }

        public T GetByKey(params object[] keyValues)
        {
            if (_tableMetadata.KeyColumns.IsNullOrEmpty())
                throw new SqlStatementException("At least one object property must have a [Key] attribute for SELECT statement by primary key to be valid.");
            if (keyValues == null)
                throw new ArgumentNullException("keyValues");
            if (keyValues.Length != _tableMetadata.KeyColumns.Count())
                throw new ArgumentOutOfRangeException("keyValues", keyValues.Length, "Number of passed key values must be equal to number of key columns.");

            return this.Query()
                .Where(this.KeyQueryWhereClause, keyValues)
                .GetSingle();
        }

        public SqlQuery<T> Query()
        {
            return _database.Query<T>();
        }
    }
}
