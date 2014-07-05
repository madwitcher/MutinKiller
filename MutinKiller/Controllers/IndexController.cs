using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using System.Web.UI;
using MySql.Data;
using System.Data;
using MySql.Data.MySqlClient;
using System.Threading;
using System.Net;

namespace MutinKiller.Controllers
{
    [OutputCache(NoStore = true, Location = OutputCacheLocation.None, Duration = 1)]
    public class IndexController : Controller
    {
        //
        // GET: /Index/

        public ActionResult Index()
        {
            return View();
        }

        public JsonResult SaveAccessToken(string accessToken)
        {
            var failed = false;
            var errorMessage = string.Empty;

            try
            {
                DataProvider.ExecuteNonQuery(string.Format(@"
update Configuration set Value = '{0}' where Name = 'AccessToken'
", accessToken));
                errorMessage = accessToken;
            }
            catch (Exception ex)
            {
                failed = true;
                errorMessage = ex.Message;
            }

            return new JsonResult { Data = new { Failed = failed, ErrorMessage = errorMessage }, JsonRequestBehavior = JsonRequestBehavior.AllowGet };
        }

        public JsonResult QueueObject(string type, string id)
        {
            var failed = false;
            var errorMessage = string.Empty;

            try
            {
                DataProvider.ExecuteNonQuery(string.Format(@"
insert into Queue(ObjectType, ObjectID) values ('{0}', {1})
", type, id));
            }
            catch (Exception ex)
            {
                failed = true;
                errorMessage = ex.Message;
            }

            return new JsonResult { Data = new { Failed = failed, ErrorMessage = errorMessage }, JsonRequestBehavior = JsonRequestBehavior.AllowGet };
        }

        public JsonResult GetStats()
        {
            var failed = false;
            var errorMessage = string.Empty;
            Stats stats = new Stats();

            try
            {
                stats.UsersCount = DataProvider.ExecuteScalar("select count(*) from Users with (nolock)").ToString();
                stats.GroupsCount = DataProvider.ExecuteScalar("select count(*) from Groups with (nolock)").ToString();
                stats.QueueLength = DataProvider.ExecuteScalar("select count(*) from Queue with (nolock)").ToString();

                DataTable logTable = DataProvider.ExecuteQuery("select * from Log with (nolock) order by ID desc");
                foreach (DataRow row in logTable.Rows)
                {
                    stats.Log.Insert(0, new KeyValue { Key = row["Timestamp"].ToString(), Value = row["Text"].ToString() });
                }
            }
            catch (Exception ex)
            {
                failed = true;
                errorMessage = ex.Message + ex.StackTrace;
            }

            return new JsonResult { Data = new { Failed = failed, ErrorMessage = errorMessage, Stats = stats }, JsonRequestBehavior = JsonRequestBehavior.AllowGet };
        }

        //public JsonResult StartExecutor()
        //{
        //    var failed = false;
        //    var errorMessage = string.Empty;
        //    Stats stats = new Stats();

        //    try
        //    {
        //        ExecutorManager.Start();
        //        Thread.Sleep(1000);

        //        stats.IsExecutorAlive = ExecutorManager.IsExecutorAlive();

        //        //stats.UsersCount = DataProvider.ExecuteScalar("select count(*) from Users").ToString();
        //        //stats.GroupsCount = DataProvider.ExecuteScalar("select count(*) from Groups").ToString();
        //        //stats.QueueLength = DataProvider.ExecuteScalar("select count(*) from Queue").ToString();

        //        //DataTable logTable = DataProvider.ExecuteQuery("select * from Log order by ID desc LIMIT 100");
        //        //foreach (DataRow row in logTable.Rows)
        //        //{
        //        //stats.Log.Insert(0, new KeyValue { Key = row["Timestamp"].ToString(), Value = row["Text"].ToString() });
        //        //}
        //    }
        //    catch (Exception ex)
        //    {
        //        failed = true;
        //        errorMessage = ex.Message;
        //    }

        //    return new JsonResult { Data = new { Failed = failed, ErrorMessage = errorMessage, Stats = stats }, JsonRequestBehavior = JsonRequestBehavior.AllowGet };
        //}

        //public JsonResult StopExecutor()
        //{
        //    var failed = false;
        //    var errorMessage = string.Empty;
        //    Stats stats = new Stats();

        //    try
        //    {
        //        ExecutorManager.StopExecutor();
        //        Thread.Sleep(1000);

        //        stats.IsExecutorAlive = ExecutorManager.IsExecutorAlive();

        //        stats.UsersCount = DataProvider.ExecuteScalar("select count(*) from Users (nolock)").ToString();
        //        stats.GroupsCount = DataProvider.ExecuteScalar("select count(*) from Groups (nolock)").ToString();
        //        stats.QueueLength = DataProvider.ExecuteScalar("select count(*) from Queue (nolock)").ToString();

        //        DataTable logTable = DataProvider.ExecuteQuery("select top 100 * from Log (nolock) order by ID desc");
        //        foreach (DataRow row in logTable.Rows)
        //        {
        //            stats.Log.Insert(0, new KeyValue { Key = row["Timestamp"].ToString(), Value = row["Text"].ToString() });
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        failed = true;
        //        errorMessage = ex.Message;
        //    }

        //    return new JsonResult { Data = new { Failed = failed, ErrorMessage = errorMessage, Stats = stats }, JsonRequestBehavior = JsonRequestBehavior.AllowGet };
        //}

        public class Stats
        {
            public bool IsExecutorAlive { get; set; }

            public string UsersCount { get; set; }
            public string GroupsCount { get; set; }

            public string QueueLength { get; set; }

            public List<KeyValue> Log { get; set; }

            public Stats()
            {
                Log = new List<KeyValue>();
            }
        }

        public class KeyValue
        {
            public string Key { get; set; }
            public string Value { get; set; }
        }

        public JsonResult GetGraphs()
        {
            var failed = false;
            var errorMessage = string.Empty;
            List<Graph> graphs = new List<Graph>();

            try
            {
                Graph platformGraph = new Graph() { Tab = "tabs-1", Header = "Platforms" };

                DataTable platformsTable = DataProvider.ExecuteQuery(@"
select u.lastseenplatform lp, count(u.lastseenplatform) as cnt from Users u (nolock)
group by u.lastseenplatform
order by cnt desc");

                foreach (DataRow row in platformsTable.Rows)
                {
                    platformGraph.Points.Add(Convert.ToInt32(row["cnt"].ToString()));
                    platformGraph.Legend.Add("%%%.%% - " + row["lp"].ToString());
                }

                graphs.Add(platformGraph);

                Graph sexGraph = new Graph() { Tab = "tabs-2", Header = "Sex" };

                DataTable sexTable = DataProvider.ExecuteQuery(@"select Sex text, count(Sex) value
from Users (nolock)
group by Sex
order by value desc");

                foreach (DataRow row in sexTable.Rows)
                {
                    sexGraph.Points.Add(Convert.ToInt32(row["value"].ToString()));
                    sexGraph.Legend.Add("%%%.%% - " + row["text"].ToString());
                }

                graphs.Add(sexGraph);

                Graph countryGraph = new Graph() { Tab = "tabs-3", Header = "Countries" };

                DataTable countryTable = DataProvider.ExecuteQuery(@"SELECT top 15 x.text, x.value
FROM (

SELECT Country text, COUNT( Country ) value
FROM Users (nolock)
GROUP BY Country
) AS x
WHERE x.text <>  ''
order by x.value desc
");

                foreach (DataRow row in countryTable.Rows)
                {
                    countryGraph.Points.Add(Convert.ToInt32(row["value"].ToString()));
                    countryGraph.Legend.Add("%%%.%% - " + row["text"].ToString());
                }

                graphs.Add(countryGraph);

                Graph cityGraph = new Graph() { Tab = "tabs-4", Header = "Cities" };

                DataTable cityTable = DataProvider.ExecuteQuery(@"SELECT top 15 x.text, x.value
FROM (

SELECT City text, COUNT( City ) value
FROM Users (nolock)
GROUP BY City
) AS x
WHERE x.text <>  ''
ORDER BY x.value DESC
");

                foreach (DataRow row in cityTable.Rows)
                {
                    cityGraph.Points.Add(Convert.ToInt32(row["value"].ToString()));
                    cityGraph.Legend.Add("%%%.%% - " + row["text"].ToString());
                }

                graphs.Add(cityGraph);

                Graph mansGraph = new Graph() { Tab = "tabs-5", Header = "Man Names" };

                DataTable mansTable = DataProvider.ExecuteQuery(@"select top 15 x.text, x.value from
(select FirstName text, count(FirstName) value
from Users (nolock)
where Sex <> 'Woman'
group by FirstName
) as x
where x.text <> ''
order by x.value desc
");

                foreach (DataRow row in mansTable.Rows)
                {
                    mansGraph.Points.Add(Convert.ToInt32(row["value"].ToString()));
                    mansGraph.Legend.Add("%%%.%% - " + row["text"].ToString());
                }

                graphs.Add(mansGraph);

                Graph womansGraph = new Graph() { Tab = "tabs-6", Header = "Woman Names" };

                DataTable womansTable = DataProvider.ExecuteQuery(@"select top 15 x.text, x.value from
(select FirstName text, count(FirstName) value
from Users (nolock)
where Sex <> 'Man'
group by FirstName
) as x
where x.text <> ''
order by x.value desc
");

                foreach (DataRow row in womansTable.Rows)
                {
                    womansGraph.Points.Add(Convert.ToInt32(row["value"].ToString()));
                    womansGraph.Legend.Add("%%%.%% - " + row["text"].ToString());
                }

                graphs.Add(womansGraph);

            }
            catch (Exception ex)
            {
                failed = true;
                errorMessage = ex.Message + ex.StackTrace;
            }

            return new JsonResult { Data = new { Failed = failed, ErrorMessage = errorMessage, Graphs = graphs }, JsonRequestBehavior = JsonRequestBehavior.AllowGet };
        }

        public ActionResult RecordIPFromServer(string ipAddress)
        {
            var failed = false;
            var errorMessage = string.Empty;
            Stats stats = new Stats();

            try
            {
                IPHostEntry host;
                string localIP = "?";
                host = Dns.GetHostEntry(Dns.GetHostName());
                foreach (IPAddress ip in host.AddressList)
                {
                    if (ip.AddressFamily.ToString() == "InterNetwork")
                    {
                        localIP = ip.ToString();
                    }
                }

                DataProvider.Log("Servers local IP Address: " + ipAddress + ", Servers request IP Address: " + HttpContext.Request.UserHostAddress + ", Servers local IP Address: " + localIP + ", IsLocal: " + HttpContext.Request.IsLocal);
            }
            catch (Exception ex)
            {
                failed = true;
                errorMessage = ex.Message;
            }

            return new JsonResult { Data = new { Failed = failed, ErrorMessage = errorMessage }, JsonRequestBehavior = JsonRequestBehavior.AllowGet };
        }

        public class Graph
        {
            public string Tab { get; set; }
            public string Header { get; set; }
            public List<int> Points { get; set; }
            public List<string> Legend { get; set; }

            public Graph()
            {
                Points = new List<int>();
                Legend = new List<string>();
            }
        }

    }


}
