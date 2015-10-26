using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using Dapper;

namespace SQL2Excel
{
    public interface ISqlRepository
    {
        string ConnectionString { get; set; }

        int CommandTimeout { get; set; }

        int? TotalRecords { get; set; }

        DataSet Execute();

        IEnumerable<T> Execute<T>(Func<IDataReader, T> func);
        
        IEnumerable<T> Execute<T>();

        int ExecuteNonQuery();

        T ExecuteScalar<T>();

        ISqlRepository WithStoredProcedure(string storedProcName);

        ISqlRepository WithSqlStatement(string sqlStatement);

        ISqlRepository AddParameter(string name, object value);

        ISqlRepository AddParameter(SqlParameter sqlParameter);

        void BulkCopy(string destinationTable, DataTable table);

        void BulkCopy(string destinationTable, IDataReader dataReader);
    }

    public class SqlRepository : ISqlRepository
    {
        public string ConnectionString { get; set; }
        public int CommandTimeout { get; set; }
        public int? TotalRecords { get; set; }

        public readonly List<SqlParameter> Parameters;

        private string cmdText;

        private bool isStoredProc;

        public SqlRepository(string connectionString)
            : this()
        {
            this.ConnectionString = connectionString;
        }

        public SqlRepository()
        {
            this.Parameters = new List<SqlParameter>();
            this.CommandTimeout = 30;
        }

        public DataSet Execute()
        {
            return (DataSet)this.Execute(string.Empty);
        }

        public IEnumerable<T> Execute<T>(Func<IDataReader, T> func)
        {
            using (var sqlConnection = CreateSqlConnection())
            {
                using (var sqlCommand = CreateSqlCommand(sqlConnection))
                {
                    sqlConnection.Open();

                    var reader = sqlCommand.ExecuteReader(CommandBehavior.Default);

                    while (reader.Read())
                    {
                        yield return func(reader);
                    }
                }
            }
        }

        public int ExecuteNonQuery()
        {
            return (int)this.Execute("ExecuteNonQuery");
        }

        public IEnumerable<T> Execute<T>()
        {
            return ExecuteToType<T>();
        }

        private IEnumerable<T> ExecuteToType<T>()
        {
            using (var sqlConnection = CreateSqlConnection())
            {
                // Add a result parameter to allow the total number of records to be returned
                this.Parameters.Add(new SqlParameter("result", 0)
                {
                    Direction = ParameterDirection.ReturnValue,
                    DbType = DbType.Int32
                });

                var p = new DynamicParameters();

                if (this.Parameters.Count != 0)
                {
                    foreach (var sqlParameter in this.Parameters.ToArray())
                    {
                        p.Add(sqlParameter.ParameterName, sqlParameter.Value, sqlParameter.DbType, sqlParameter.Direction);
                    }
                }

                IEnumerable<T> r;
                
                r = sqlConnection.Query<T>(this.cmdText, p, commandType: this.isStoredProc ? CommandType.StoredProcedure : CommandType.Text).ToList();
                
                this.TotalRecords = this.isStoredProc ? p.Get<int?>("result") : r.Count();

                return r;
            }
        }

        public T ExecuteScalar<T>()
        {
            return (T)this.Execute("ExecuteScalar");
        }

        public ISqlRepository WithStoredProcedure(string storedProcName)
        {
            this.isStoredProc = true;
            this.Parameters.Clear();
            this.cmdText = storedProcName;

            return this;
        }

        public ISqlRepository WithSqlStatement(string sqlStatement)
        {
            this.isStoredProc = false;
            this.Parameters.Clear();
            this.cmdText = sqlStatement;
            return this;
        }

        public ISqlRepository AddParameter(string name, object value)
        {
            this.AddParameter(new SqlParameter(name, value));

            return this;
        }

        public ISqlRepository AddParameter(SqlParameter sqlParameter)
        {
            this.Parameters.Add(sqlParameter);

            return this;
        }

        public void BulkCopy(string destinationTable, IDataReader dataReader)
        {
            using (var sqlConnection = CreateSqlConnection())
            {
                sqlConnection.Open();
                using (var bc = new SqlBulkCopy(sqlConnection))
                {
                    bc.DestinationTableName = destinationTable;
                    bc.WriteToServer(dataReader);
                }
            }
        }

        public void BulkCopy(string destinationTable, DataTable table)
        {
            using (var sqlConnection = CreateSqlConnection())
            {
                sqlConnection.Open();
                using (var bc = new SqlBulkCopy(sqlConnection))
                {
                    bc.DestinationTableName = destinationTable;
                    bc.WriteToServer(table);
                }
            }
        }

        private Object Execute(string type)
        {
            using (var sqlConnection = CreateSqlConnection())
            {
                using (var sqlCommand = CreateSqlCommand(sqlConnection))
                {
                    sqlConnection.Open();

                    if (type.Equals("ExecuteNonQuery"))
                    {
                        return sqlCommand.ExecuteNonQuery();
                    }

                    if (type.Equals("ExecuteScalar"))
                    {
                        return sqlCommand.ExecuteScalar();
                    }

                    var returnDataSet = new DataSet("Results");
                    var sqlDataAdaptor = new SqlDataAdapter(sqlCommand);

                    sqlDataAdaptor.Fill(returnDataSet);

                    return returnDataSet;
                }
            }
        }

        private SqlConnection CreateSqlConnection()
        {
            return new SqlConnection(this.GetConnectionString());
        }

        private SqlCommand CreateSqlCommand(SqlConnection sqlConnection)
        {
            var sqlCommand = new SqlCommand(this.cmdText, sqlConnection)
            {
                CommandTimeout = this.CommandTimeout,
                CommandType = this.isStoredProc ? CommandType.StoredProcedure : CommandType.Text
            };

            if (this.Parameters.Count != 0)
            {
                sqlCommand.Parameters.AddRange(this.Parameters.ToArray());
            }

            return sqlCommand;
        }

        private string GetConnectionString()
        {
            return this.ConnectionString ?? ConfigurationManager.ConnectionStrings["SqlRepository"].ConnectionString;
        }
    }
}