using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data;
using System.Configuration;
using System.Data.SqlClient;
using System.Text;
using System.Threading;

namespace MutinKiller
{
    internal static class DataProvider
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["MSSQL"].ConnectionString;

        internal static DataTable ExecuteQuery(string query)
        {
            DataTable table = null;

            try
            {
                using (var conn = new SqlConnection(ConnectionString))
                {
                    var opened = false;

                    while (!opened)
                    {
                        try
                        {
                            conn.Open();
                            opened = true;
                        }
                        catch { }
                    }

                    var completed = false;

                    while (!completed)
                    {
                        try
                        {
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

                            completed = true;
                        }
                        catch (Exception ex)
                        {
                            if (!ex.Message.Contains("Timeout expired."))
                            {
                                throw new Exception("DB Error", ex);
                            }
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

        internal static void WriteUsersToServer(DataTable users, List<string> preparedIds, string groupID)
        {
            try
            {

                using (var conn = new SqlConnection(ConnectionString))
                {
                    var opened = false;

                    while (!opened)
                    {
                        try
                        {
                            conn.Open();
                            opened = true;
                        }
                        catch { }
                    }

                    StringBuilder query = new StringBuilder();
                    foreach (string id in preparedIds)
                    {
                        query.AppendLine(string.Format(@"delete from Users where ID = {0}
delete from Users2Groups where UserID = {0} and GroupID = {1}", id, groupID));
                    }

                    var finalQuery = query.ToString();
                    if (!string.IsNullOrEmpty(finalQuery))
                    {
                        bool completed = false;

                        while (!completed)
                        {
                            try
                            {
                                SqlCommand finalCmd = new SqlCommand(finalQuery.ToString(), conn);
                                finalCmd.CommandTimeout = 0;
                                finalCmd.ExecuteNonQuery();
                                completed = true;
                            }
                            catch (Exception ex)
                            {
                                if (!ex.Message.Contains("Timeout expired."))
                                {
                                    throw new Exception("DB Error", ex);
                                }
                            }
                        }

                    }

                    Thread.Sleep(1000);

                    using (SqlBulkCopy sbc = new SqlBulkCopy(conn))
                    {
                        sbc.DestinationTableName = "Users";
                        sbc.ColumnMappings.Add(0, 0);
                        sbc.ColumnMappings.Add(1, 1);
                        sbc.ColumnMappings.Add(2, 2);
                        sbc.ColumnMappings.Add(3, 3);
                        sbc.ColumnMappings.Add(4, 4);
                        sbc.ColumnMappings.Add(5, 5);
                        sbc.ColumnMappings.Add(6, 6);
                        sbc.ColumnMappings.Add(7, 7);
                        sbc.ColumnMappings.Add(8, 8);
                        sbc.ColumnMappings.Add(9, 9);
                        sbc.ColumnMappings.Add(10, 10);
                        sbc.ColumnMappings.Add(11, 11);
                        sbc.ColumnMappings.Add(12, 12);
                        sbc.ColumnMappings.Add(13, 13);
                        sbc.ColumnMappings.Add(14, 14);
                        sbc.WriteToServer(users);
                        sbc.Close(); 
                    }

                    var users2Groups = new DataTable();
                    users2Groups.Columns.Add(new DataColumn("UserID", typeof(Int32)));
                    users2Groups.Columns.Add(new DataColumn("GroupID", typeof(Int32)));

                    foreach (string id in preparedIds)
                        users2Groups.Rows.Add(id, groupID);

                    Thread.Sleep(1000);

                    using (SqlBulkCopy sbc = new SqlBulkCopy(conn))
                    {
                        sbc.DestinationTableName = "Users2Groups";
                        sbc.ColumnMappings.Add(0, 0);
                        sbc.ColumnMappings.Add(1, 1);
                        sbc.WriteToServer(users2Groups);
                        sbc.Close();
                    }
                }

            }
            catch (Exception ex)
            {
                DataProvider.Log("DB Error: " + ex.Message);
            }
        }

        internal static void ExecuteNonQuery(string query)
        {
            try
            {

                using (var conn = new SqlConnection(ConnectionString))
                {
                    var opened = false;

                    while (!opened)
                    {
                        try
                        {
                            conn.Open();
                            opened = true;
                        }
                        catch { }
                    }

                    var completed = false;

                    while (!completed)
                    {
                        try
                        {
                            var command = new SqlCommand(query, conn);

                            command.CommandTimeout = 0;
                            command.ExecuteNonQuery();

                            completed = true;
                        }
                        catch (Exception ex)
                        {
                            if (!ex.Message.Contains("Timeout expired."))
                            {
                                throw new Exception("DB Error", ex);
                            }
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                DataProvider.Log("DB Error: " + ex.Message);
            }
        }



        internal static void ExecuteNonQuery(string query, SqlConnection conn)
        {
            try
            {

                var command = new SqlCommand(query, conn);

                command.CommandTimeout = 0;

                command.ExecuteNonQuery();

            }
            catch (Exception ex)
            {
                DataProvider.Log("DB Error: " + ex.Message);
            }
        }

        internal static object ExecuteScalar(string query)
        {
            object scalarValue = null;

            try
            {
                using (var conn = new SqlConnection(ConnectionString))
                {
                    var opened = false;

                    while (!opened)
                    {
                        try
                        {
                            conn.Open();
                            opened = true;
                        }
                        catch { }
                    }

                    var completed = false;

                    while (!completed)
                    {
                        try
                        {
                            var command = new SqlCommand(query, conn);

                            command.CommandTimeout = 0;

                            scalarValue = command.ExecuteScalar();

                            completed = true;
                        }
                        catch (Exception ex)
                        {
                            if (!ex.Message.Contains("Timeout expired."))
                            {
                                throw new Exception("DB Error", ex);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                DataProvider.Log("DB Error: " + ex.Message);
            }

            return scalarValue;
        }

        internal static int LogAndGetLogId(string text)
        {
            int logId = 0;
            try
            {
                var lastInsertId = ExecuteScalar(string.Format(@"insert into Log(Timestamp, Text) values('{0}', N'{1}');
select scope_identity()", DateTime.Now.AddHours(2).ToString("yyyy-MM-dd HH:mm:ss"), text));

                if (lastInsertId == null || !Int32.TryParse(lastInsertId.ToString(), out logId))
                    return 0;
            }
            catch (Exception ex)
            {
                //ExecuteNonQuery("Error writing the log entry: " + ex.Message);
            }

            return logId;
        }

        internal static void LogWithLogId(string text, int logId)
        {
            try
            {
                ExecuteNonQuery(string.Format("update Log set Text = N'{0}' where ID = {1}", text, logId));
            }
            catch (Exception ex)
            {
                //ExecuteNonQuery("Error writing the log entry: " + ex.Message);
            }
        }

        internal static void Log(string text)
        {
            try
            {
                using (var conn = new SqlConnection(ConnectionString))
                {
                    var query = string.Format("insert into Log(Timestamp, Text) values('{0}', N'{1}')", DateTime.Now.AddHours(2).ToString("yyyy-MM-dd HH:mm:ss"), text);

                    var opened = false;

                    while (!opened)
                    {
                        try
                        {
                            conn.Open();
                            opened = true;
                        }
                        catch { }
                    }

                    var completed = false;

                    while (!completed)
                    {
                        try
                        {
                            var command = new SqlCommand(query, conn);

                            command.CommandTimeout = 0;

                            command.ExecuteNonQuery();

                            completed = true;
                        }
                        catch (Exception ex)
                        {
                            if (!ex.Message.Contains("Timeout expired."))
                            {
                                throw new Exception("DB Error", ex);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                //ExecuteNonQuery("Error writing the log entry: " + ex.Message);
            }
        }
    }
}