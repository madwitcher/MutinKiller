using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using MySql.Data;
using MySql.Data.MySqlClient;
using System.Data;
using System.Configuration;
using System.Data.SqlClient;

namespace MutinKiller
{
    internal static class DataProvider
    {
        private static readonly string ConnectionString = "Password=Password1@;Persist Security Info=True;User ID=sa;Initial Catalog=Mutin;Data Source=89.252.34.204;";

        internal static DataTable ExecuteQuery(string query)
        {
            DataTable table = null;

            try
            {
                using (var conn = new SqlConnection(ConnectionString))
                {
                    conn.Open();

                    var command = new SqlCommand(query, conn);
                    command.CommandTimeout = 0;

                    using (var reader = command.ExecuteReader())
                    {

                        var schemaTable = reader.GetSchemaTable();

                        var columns = new List<DataColumn>();
                        table = new DataTable();

                        foreach (DataRow row in schemaTable.Rows)
                        {
                            table.Columns.Add(new DataColumn { ColumnName = row["ColumnName"].ToString() });
                        }

                        while (reader.Read())
                        {
                            var o = new object[reader.FieldCount];
                            reader.GetValues(o);
                            table.Rows.Add(o);
                        }

                    }
                }
            }
            catch (Exception ex)
            {
                DataProvider.Log("DB Error: " + ex.Message);
            }

            return table;
        }

        internal static void ExecuteNonQuery(string query)
        {
            try
            {

                using (var conn = new SqlConnection(ConnectionString))
                {
                    conn.Open();

                    var command = new SqlCommand(query, conn);
                    command.CommandTimeout = 0;

                    command.ExecuteNonQuery();
                }

            }
            catch (Exception ex)
            {
                //DataProvider.Log("DB Error: " + ex.Message);
            }
        }

        internal static object ExecuteScalar(string query)
        {
            object scalarValue = null;

            try
            {
                using (var conn = new SqlConnection(ConnectionString))
                {
                    conn.Open();

                    var command = new SqlCommand(query, conn);
                    command.CommandTimeout = 0;
                    scalarValue = command.ExecuteScalar();
                }
            }
            catch (Exception ex)
            {
                DataProvider.Log("DB Error: " + ex.Message);
            }

            return scalarValue;
        }

        internal static void Log(string text)
        {
            try
            {
                ExecuteNonQuery(string.Format("insert into Log(Timestamp, Text) values('{0}', N'{1}')", DateTime.Now.AddHours(2).ToString("yyyy-MM-dd HH:mm:ss"), text));
            }
            catch (Exception ex)
            {
                //ExecuteNonQuery("Error writing the log entry: " + ex.Message);
            }
        }
    }
}