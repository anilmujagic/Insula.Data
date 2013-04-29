using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using Insula.Common;

namespace Insula.Data.DatabaseServices
{
    public class DatabaseSchemaInfo
    {
        private DatabaseSchemaInfo()
        {
            _tables = new List<TableInfo>();
        }

        private List<TableInfo> _tables;
        public IEnumerable<TableInfo> Tables
        {
            get
            {
                return _tables.AsReadOnly();
            }
        }

        public static DatabaseSchemaInfo LoadFromDatabase(string connectionString)
        {
            var dbSchemaInfo = new DatabaseSchemaInfo();

            using (var connection = new SqlConnection(connectionString))
            {
                dbSchemaInfo._tables.AddRange(LoadTables(connection));
            }

            return dbSchemaInfo;
        }

        private static IEnumerable<TableInfo> LoadTables(SqlConnection connection)
        {
            var tables = new List<TableInfo>();

            var command = connection.CreateCommand();
            connection.Open();


            //Load tables

            command.CommandText = @"
                SELECT 
                    TABLE_CATALOG AS [Database],
                    TABLE_SCHEMA AS [Schema],
                    TABLE_NAME AS [Name]
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
                ";

            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    var table = new TableInfo();
                    table.Name = (string)reader["Name"];
                    table.Schema = (string)reader["Schema"];
                    table.Database = (string)reader["Database"];
                    tables.Add(table);
                }
            }


            //Load columns

            command.Parameters.Add(new SqlParameter("@TableName", null));

            foreach (var t in tables)
            {
                command.CommandText = @"
                    SELECT 
                        COLUMN_NAME AS Name, 
                        DATA_TYPE AS DataType, 
                        CHARACTER_MAXIMUM_LENGTH AS MaxLength, 
                        NUMERIC_SCALE AS DecimalPlaces,
                        ORDINAL_POSITION AS OrdinalPosition, 
                        CASE IS_NULLABLE
                            WHEN 'YES' THEN CAST(1 AS BIT)
                            WHEN 'NO' THEN CAST(0 AS BIT)
                        END AS IsNullable, 
                        CAST(COLUMNPROPERTY(object_id('[' + TABLE_SCHEMA + '].[' + TABLE_NAME + ']'), COLUMN_NAME, 'IsIdentity') AS BIT) AS IsIdentity,
                        CAST(COLUMNPROPERTY(object_id('[' + TABLE_SCHEMA + '].[' + TABLE_NAME + ']'), COLUMN_NAME, 'IsComputed') AS BIT) AS IsComputed
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = @TableName
                    ORDER BY OrdinalPosition ASC
                    ";
                command.Parameters["@TableName"].Value = t.Name;

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var column = new ColumnInfo();
                        column.Name = reader["Name"].ToString();
                        column.DataType = reader["DataType"].ToString();
                        column.MaxLength = (int?)(reader["MaxLength"] != DBNull.Value ? (int?)reader["MaxLength"] : null);
                        column.DecimalPlaces = (int?)(reader["DecimalPlaces"] != DBNull.Value ? (int?)reader["DecimalPlaces"] : null);
                        column.IsNullable = (bool)reader["IsNullable"];
                        column.IsIdentity = (bool)reader["IsIdentity"];
                        column.IsComputed = (bool)reader["IsComputed"];
                        column.OrdinalPosition = (int)reader["OrdinalPosition"];
                        t.AddColumn(column);
                    }
                }


                //Load primary key columns

                command.CommandText = @"
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS C
                    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS U
                        ON C.TABLE_NAME = U.TABLE_NAME
                        AND C.CONSTRAINT_NAME = U.CONSTRAINT_NAME	
                    WHERE C.CONSTRAINT_TYPE = 'PRIMARY KEY'
                    AND C.TABLE_NAME = @TableName
                    ORDER BY U.ORDINAL_POSITION ASC
                    ";

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        t.Columns.Single(c => c.Name == reader[0].ToString()).IsPrimaryKey = true;
                    }
                }
            }


            return tables;
        }
    }
}
