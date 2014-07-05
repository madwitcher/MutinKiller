using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data;
using System.Threading;
using System.Net;
using System.Runtime.Serialization.Json;
using System.Runtime.Serialization;
using System.ComponentModel;
using System.IO;
using System.Web.Hosting;
using System.Text;

namespace MutinKiller
{
    public class Executor
    {
        private static readonly object _lock = new object();
        private static BackgroundWorker _dbWorker = new BackgroundWorker();

        private const string API_VERSION = "5.21";
        private const string API_LANGUAGE = "en";
        private const string EXECUTOR_THREAD = "EXECUTOR_THREAD";

        static void Main(string[] args)
        {
            AppDomain.CurrentDomain.ProcessExit += new EventHandler(CurrentDomain_ProcessExit);
            new Executor().StartInternal();

        }

        static void CurrentDomain_ProcessExit(object sender, EventArgs e)
        {
            DataProvider.Log("Exiting executor...");
        }

        internal enum ExecutorStatus
        {
            Killed = 0,
            Alive = 1,
            Stopping = 2,
            Unknown = 4
        }

        private static bool m_stopRequested = false;

        private void StartInternal()
        {
            try
            {
                DataProvider.Log("Executor was started.");

                _dbWorker.DoWork += new DoWorkEventHandler(worker_DoWork);
                _dbWorker.RunWorkerCompleted += new RunWorkerCompletedEventHandler(worker_RunWorkerCompleted);

                System.Threading.Thread.CurrentThread.CurrentCulture = new System.Globalization.CultureInfo("ru-RU");

                while (!m_stopRequested)
                {
                    try
                    {
                        PollQueue();
                    }
                    catch (Exception ex)
                    {
                        DataProvider.Log(ex.Message + ". Stack Trace: " + ex.StackTrace);
                        Thread.Sleep(5000);
                    }
                }
            }
            catch (ThreadAbortException ex)
            {
                DataProvider.Log("Executor was aborted by the system, restarting...");
            }
            catch (Exception ex)
            {
                DataProvider.Log("Something is gone bad... Details: " + ex.Message);
            }

            m_stopRequested = false;

            return;
        }

        private void PollQueue()
        {
            try
            {
                var pollQueueTable = DataProvider.ExecuteQuery(string.Format("select top 1 * from Queue"));
                if (pollQueueTable == null || pollQueueTable.Rows.Count == 0)
                {
                    Thread.Sleep(5000);
                    return;
                }

                var objectType = pollQueueTable.Rows[0]["ObjectType"].ToString();
                var objectId = pollQueueTable.Rows[0]["ObjectID"].ToString();
                var recordId = pollQueueTable.Rows[0]["ID"].ToString();

                if (objectType == "group")
                {
                    ScanGroup(objectId, recordId);
                }
                else
                {
                    ScanUser(objectId, recordId);
                }
            }
            catch (Exception ex)
            {
                DataProvider.Log("Unexpected error occured during polling. Details: " + ex.Message);
            }

            Thread.Sleep(5000);
            return;
        }

        private DataTable PrepareUsersTable()
        {
            var table = new DataTable();

            table.Columns.Add(new DataColumn("ID", typeof(Int32)));
            table.Columns.Add(new DataColumn("FirstName", typeof(string)));
            table.Columns.Add(new DataColumn("LastName", typeof(string)));
            table.Columns.Add(new DataColumn("Verified", typeof(Int32)));
            table.Columns.Add(new DataColumn("Sex", typeof(string)));
            table.Columns.Add(new DataColumn("BDate", typeof(string)));
            table.Columns.Add(new DataColumn("City", typeof(string)));
            table.Columns.Add(new DataColumn("Country", typeof(string)));
            table.Columns.Add(new DataColumn("Site", typeof(string)));
            table.Columns.Add(new DataColumn("Status", typeof(string)));
            table.Columns.Add(new DataColumn("LastSeenTime", typeof(string)));
            table.Columns.Add(new DataColumn("LastSeenPlatform", typeof(string)));
            table.Columns.Add(new DataColumn("NickName", typeof(string)));
            table.Columns.Add(new DataColumn("ScanDate", typeof(string)));
            table.Columns.Add(new DataColumn("PoliticalView", typeof(string)));

            return table;
        }

        private void ScanGroup(string id, string recordId)
        {
            var logId = DataProvider.LogAndGetLogId("Scan of group with ID: " + id + " was started. Progress: 0%");
            var groupInfoResponse = ExecuteRequest<GroupInfoResponse>("groups.getById", string.Format("group_id={0}&fields=city,country,description,members_count,status,verified,site", id), string.Empty);

            if (m_stopRequested)
            {
                DataProvider.Log("Stop was requested.");
                m_stopRequested = false;
                return;
            }

            if (groupInfoResponse.Error != null)
            {
                DataProvider.ExecuteNonQuery("delete from Queue where ID = " + recordId);
                DataProvider.Log("Error getting group info for group ID: " + id + string.Format(". Details: ErrorCode - {0}, ErrorMessage - {1}",
                    groupInfoResponse.Error.ErrorCode, groupInfoResponse.Error.ErrorMessage));
            }
            else if (groupInfoResponse.Groups == null || groupInfoResponse.Groups.Length != 1)
            {
                DataProvider.ExecuteNonQuery("delete from Queue where ID = " + recordId);
                DataProvider.Log("Error getting group info for group ID: " + id + ". Unspecified error (response is null or groups count != 1)");
            }
            else
            {
                DataProvider.LogWithLogId("Scan of group with ID: " + id + " was started. Progress: 0%", logId);
                Thread.Sleep(200);


                if (m_stopRequested)
                {
                    m_stopRequested = false;
                    return;
                }

                var group = groupInfoResponse.Groups[0];

                DataProvider.ExecuteNonQuery(string.Format(@"delete from Groups where ID = {0}", group.ID));

                DataProvider.ExecuteNonQuery(string.Format(@"insert into Groups(ID, Name, IsClosed, Deactivated, Type, City, Country, Description, MembersCount, Status, Site, Verified, ScanDate)
values({0}, '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', {8}, '{9}', '{10}', {11}, '{12}')", group.ID, group.Name.Replace("'", "''"), group.IsClosed == 0 ? "Open" : group.IsClosed == 1 ? "Closed" : "Private",
                                                                                                   group.Deactivated ?? string.Empty, group.Type, group.City != null ? group.City.Title.Replace("'", "''") : string.Empty,
                                                                                                   group.Country != null ? group.Country.Title.Replace("'", "''") : string.Empty,
                                                                                                   string.IsNullOrWhiteSpace(group.Description) ? string.Empty : group.Description.Replace("'", "''"), group.MembersCount, string.IsNullOrWhiteSpace(group.Status) ? string.Empty : group.Status.Replace("'", "''"),
                                                                                                   string.IsNullOrWhiteSpace(group.Site) ? string.Empty : group.Site.Replace("'", "''"),
                                                                                                   group.Verified, DateTime.Now.AddHours(2).ToString("yyyy-MM-dd HH:mm:ss")));

                var users = PrepareUsersTable();

                var isReadyForRecording = true;

                for (var offset = 0; offset < groupInfoResponse.Groups[0].MembersCount; offset = offset + 1000)
                {

                    DateTime timeStart = DateTime.Now;

                    try
                    {

                        var groupGetMembersResponse = ExecuteRequest<GroupGetMembersResponseWrapper>("groups.getMembers",
                            string.Format("group_id={0}&fields=sex,bdate,city,country,site,status,last_seen,nickname,verified&offset={1}&count=1000", id, offset),
                            string.Empty);

                        if (m_stopRequested)
                        {
                            DataProvider.Log("Stop was requested.");

                            m_stopRequested = false;
                            return;
                        }

                        if (groupGetMembersResponse.Error != null)
                        {
                            DataProvider.ExecuteNonQuery("delete from Queue where ID = " + recordId);

                            DataProvider.Log("Error getting user info for group ID: " + id + string.Format(". Details: ErrorCode - {0}, ErrorMessage - {1}",
                                groupGetMembersResponse.Error.ErrorCode, groupGetMembersResponse.Error.ErrorMessage));
                            isReadyForRecording = false;
                            break;
                        }
                        else if (groupGetMembersResponse.Response.Users == null)
                        {
                            DataProvider.ExecuteNonQuery("delete from Queue where ID = " + recordId);

                            DataProvider.Log("Error getting user info for group ID: " + id + ". Unspecified error (response is null)");
                            isReadyForRecording = false;
                            break;
                        }
                        else if (groupGetMembersResponse.Response.Users.Length > 0)
                        {
                            FetchInTable(users, groupGetMembersResponse.Response.Users);

                            DataProvider.LogWithLogId("Scan of group with ID: " + id + string.Format(" was started. Progress: {0}%", GetPersentageString(offset, groupInfoResponse.Groups[0].MembersCount)), logId);
                        }

                        if (offset > 0 && offset % 10000 == 0)
                        {
                            while (_dbWorker.IsBusy)
                                Thread.Sleep(1000);

                            _dbWorker.RunWorkerAsync(new DbWorkerParams { GroupID = id, UserIDs = GetPreparedIds(users), Users = users });

                            users = PrepareUsersTable();
                        }
                    }
                    catch (Exception ex)
                    {
                        DataProvider.Log(string.Format("Error getting users from {0} to {1} info for group ID: ", offset, (offset + 1000)) + id + ". Error: " + ex.Message);
                    }

                    DateTime timeEnd = DateTime.Now;

                    TimeSpan diff = timeEnd - timeStart;

                    if (diff.Milliseconds > 200)
                        continue;
                    Thread.Sleep(200);
                }

                if (users.Rows.Count > 0)
                {
                    while (_dbWorker.IsBusy)
                        Thread.Sleep(1000);

                    DataProvider.WriteUsersToServer(users, GetPreparedIds(users), group.ID.ToString());

                    users = PrepareUsersTable();
                }

                if (m_stopRequested)
                {
                    DataProvider.Log("Stop was requested.");
                    m_stopRequested = false;
                    return;
                }

                if (isReadyForRecording)
                {
                    DataProvider.ExecuteNonQuery("delete from Queue where ID = " + recordId);

                    DataProvider.LogWithLogId("Scan of group with ID: " + id + " was finished. Progress: 100%", logId);
                }
                else
                {
                    DataProvider.Log("Records are not ready. Bug");
                }
            }

            return;
        }

        private void worker_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            
        }

        private void worker_DoWork(object sender, DoWorkEventArgs e)
        {
            DbWorkerParams parameters = e.Argument as DbWorkerParams;

            DataProvider.WriteUsersToServer(parameters.Users, parameters.UserIDs, parameters.GroupID);
        }

        private List<string> GetPreparedIds(DataTable users)
        {
            List<string> ids = new List<string>();

            foreach (DataRow row in users.Rows)
                ids.Add(row["ID"].ToString());

            return ids;
        }

        private void FetchInTable(DataTable users, User[] usersArray)
        {
            foreach (User user in usersArray)
            {
                users.Rows.Add(user.ID, user.FirstName, user.LastName, user.Verified, user.Sex == 1 ? "Woman" : user.Sex == 2 ? "Man" : "N/A",
                    string.IsNullOrWhiteSpace(user.BDate) ? string.Empty : user.BDate, user.City != null ? user.City.Title : string.Empty, user.Country != null ? user.Country.Title : string.Empty,
                    string.IsNullOrWhiteSpace(user.Site) ? string.Empty : user.Site, string.IsNullOrWhiteSpace(user.Status) ? string.Empty : user.Status,
                    user.LastSeenInfo != null ?
                    new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc).AddSeconds(user.LastSeenInfo.Time).ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss") :
                    new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc).ToString("yyyy-MM-dd HH:mm:ss"), user.LastSeenInfo == null ? "N/A" :
                    user.LastSeenInfo.Platform == 1 ? "mobile" :
                    user.LastSeenInfo.Platform == 2 ? "iphone" :
                    user.LastSeenInfo.Platform == 3 ? "ipad" :
                    user.LastSeenInfo.Platform == 4 ? "android" :
                    user.LastSeenInfo.Platform == 5 ? "wphone" :
                    user.LastSeenInfo.Platform == 6 ? "windows" :
                    user.LastSeenInfo.Platform == 7 ? "web" : "N/A",
                    string.IsNullOrWhiteSpace(user.NickName) ? string.Empty : user.NickName,
                    DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), "");
            }
        }



        private void ScanGroup_Obsolete(string id, string recordId)
        {
            //var accessToken = DataProvider.ExecuteScalar("select Value from Configuration where Name = 'AccessToken'").ToString();
            var logId = DataProvider.LogAndGetLogId("Scan of group with ID: " + id + " was started. Progress: 0%");
            var groupInfoResponse = ExecuteRequest<GroupInfoResponse>("groups.getById", string.Format("group_id={0}&fields=city,country,description,members_count,status,verified,site", id), string.Empty);

            if (m_stopRequested)
            {
                DataProvider.Log("Stop was requested.");
                m_stopRequested = false;
                return;
            }

            if (groupInfoResponse.Error != null)
            {
                DataProvider.ExecuteNonQuery("delete from Queue where ID = " + recordId);
                DataProvider.Log("Error getting group info for group ID: " + id + string.Format(". Details: ErrorCode - {0}, ErrorMessage - {1}",
                    groupInfoResponse.Error.ErrorCode, groupInfoResponse.Error.ErrorMessage));
            }
            else if (groupInfoResponse.Groups == null || groupInfoResponse.Groups.Length != 1)
            {
                DataProvider.ExecuteNonQuery("delete from Queue where ID = " + recordId);
                DataProvider.Log("Error getting group info for group ID: " + id + ". Unspecified error (response is null or groups count != 1)");
            }
            else
            {
                DataProvider.LogWithLogId("Scan of group with ID: " + id + " was started. Progress: 1%", logId);
                Thread.Sleep(200);
                var usersInGroup = new List<User>();
                var isReadyForRecording = true;

                for (var offset = 0; offset < groupInfoResponse.Groups[0].MembersCount; offset = offset + 1000)
                {

                    DateTime timeStart = DateTime.Now;

                    try
                    {

                        var groupGetMembersResponse = ExecuteRequest<GroupGetMembersResponseWrapper>("groups.getMembers",
                            string.Format("group_id={0}&fields=sex,bdate,city,country,site,status,last_seen,nickname,verified&offset={1}&count=1000", id, offset),
                            string.Empty);

                        if (m_stopRequested)
                        {
                            DataProvider.Log("Stop was requested.");
                            m_stopRequested = false;
                            return;
                        }

                        if (groupGetMembersResponse.Error != null)
                        {
                            DataProvider.ExecuteNonQuery("delete from Queue where ID = " + recordId);
                            DataProvider.Log("Error getting user info for group ID: " + id + string.Format(". Details: ErrorCode - {0}, ErrorMessage - {1}",
                                groupGetMembersResponse.Error.ErrorCode, groupGetMembersResponse.Error.ErrorMessage));
                            isReadyForRecording = false;
                            break;
                        }
                        else if (groupGetMembersResponse.Response.Users == null)
                        {
                            DataProvider.ExecuteNonQuery("delete from Queue where ID = " + recordId);
                            DataProvider.Log("Error getting user info for group ID: " + id + ". Unspecified error (response is null)");
                            isReadyForRecording = false;
                            break;
                        }
                        else if (groupGetMembersResponse.Response.Users.Length > 0)
                        {
                            usersInGroup.AddRange(groupGetMembersResponse.Response.Users);
                            DataProvider.LogWithLogId("Scan of group with ID: " + id + string.Format(" was started. Progress: {0}%", GetPersentageString(offset, groupInfoResponse.Groups[0].MembersCount)), logId);
                        }
                    }
                    catch (Exception ex)
                    {
                        DataProvider.Log(string.Format("Error getting users from {0} to {1} info for group ID: ", offset, (offset + 1000)) + id + ". Error: " + ex.Message);
                    }

                    DateTime timeEnd = DateTime.Now;

                    TimeSpan diff = timeEnd - timeStart;

                    if (diff.Milliseconds > 200)
                        continue;
                    Thread.Sleep(200);
                }

                if (m_stopRequested)
                {
                    DataProvider.Log("Stop was requested.");
                    m_stopRequested = false;
                    return;
                }

                if (isReadyForRecording)
                {
                    DataProvider.LogWithLogId("Scan of group with ID: " + id + " was started. Progress: 100%", logId);

                    RecordGroupScanResults(groupInfoResponse.Groups[0], usersInGroup, recordId);
                }
                else
                {
                    DataProvider.Log("Records are not ready. Bug");
                }
            }

            return;
        }

        private string GetPersentageString(int part, int total)
        {
            if (total == 0)
                return "0";

            return ((part * 100) / total).ToString();
        }

        private void RecordGroupScanResults(Group group, List<User> usersInGroup, string recordId)
        {
            if (m_stopRequested)
            {
                m_stopRequested = false;
                return;
            }


            var logId = DataProvider.LogAndGetLogId("Scan of group with ID: " + group.ID + " was finished. Writing the results... Progress: 0%");


            DataProvider.ExecuteNonQuery(string.Format(@"delete from Groups where ID = {0}", group.ID));


            if (m_stopRequested)
            {
                m_stopRequested = false;
                return;
            }

            DataProvider.ExecuteNonQuery(string.Format(@"insert into Groups(ID, Name, IsClosed, Deactivated, Type, City, Country, Description, MembersCount, Status, Site, Verified, ScanDate)
values({0}, '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', {8}, '{9}', '{10}', {11}, '{12}')", group.ID, group.Name.Replace("'", "''"), group.IsClosed == 0 ? "Open" : group.IsClosed == 1 ? "Closed" : "Private",
                                                                                               group.Deactivated ?? string.Empty, group.Type, group.City != null ? group.City.Title.Replace("'", "''") : string.Empty,
                                                                                               group.Country != null ? group.Country.Title.Replace("'", "''") : string.Empty,
                                                                                               string.IsNullOrWhiteSpace(group.Description) ? string.Empty : group.Description.Replace("'", "''"), group.MembersCount, string.IsNullOrWhiteSpace(group.Status) ? string.Empty : group.Status.Replace("'", "''"),
                                                                                               string.IsNullOrWhiteSpace(group.Site) ? string.Empty : group.Site.Replace("'", "''"),
                                                                                               group.Verified, DateTime.Now.AddHours(2).ToString("yyyy-MM-dd HH:mm:ss")));

            DataProvider.Log("Inserting batch for users in group ID: " + group.ID + "...");
            var insertUsersQuery = new StringBuilder();

            int usersCounter = 0;

            foreach (User user in usersInGroup)
            {
                if (m_stopRequested)
                {
                    m_stopRequested = false;
                    return;
                }

                if (usersCounter % 1000 == 0)
                {
                    DataProvider.LogWithLogId("Scan of group with ID: " + group.ID + string.Format(" was finished. Writing the results... Progress: {0}%", GetPersentageString(usersCounter, usersInGroup.Count)), logId);
                }

                var tempQuery = new StringBuilder();

                tempQuery.Append(string.Format(@"delete from Users where ID = {0};", user.ID) + Environment.NewLine);
                tempQuery.Append(string.Format(@"insert into Users(ID, FirstName, LastName, Verified, Sex, BDate, City, Country, Site, Status, LastSeenTime, LastSeenPlatform, NickName, ScanDate)
values({0}, '{1}', '{2}', {3}, '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', '{10}', '{11}', '{12}', '{13}');
", user.ID, user.FirstName.Replace("'", "''"), user.LastName.Replace("'", "''"), user.Verified, user.Sex == 1 ? "Woman" : user.Sex == 2 ? "Man" : "N/A",
                                                            user.BDate, user.City != null ? user.City.Title.Replace("'", "''") : string.Empty,
                                                            user.Country != null ? user.Country.Title.Replace("'", "''") : string.Empty,
                                                            string.IsNullOrWhiteSpace(user.Site) ? string.Empty : user.Site.Replace("'", "''"),
                                                            string.IsNullOrWhiteSpace(user.Status) ? string.Empty : user.Status.Replace("'", "''"), user.LastSeenInfo != null ?
                                                            new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc).AddSeconds(user.LastSeenInfo.Time).ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss") :
                                                            new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc).ToString("yyyy-MM-dd HH:mm:ss"), user.LastSeenInfo == null ? "N/A" :
                                                            user.LastSeenInfo.Platform == 1 ? "mobile" :
                                                            user.LastSeenInfo.Platform == 2 ? "iphone" :
                                                            user.LastSeenInfo.Platform == 3 ? "ipad" :
                                                            user.LastSeenInfo.Platform == 4 ? "android" :
                                                            user.LastSeenInfo.Platform == 5 ? "wphone" :
                                                            user.LastSeenInfo.Platform == 6 ? "windows" :
                                                            user.LastSeenInfo.Platform == 7 ? "web" : "N/A",
                                                            string.IsNullOrWhiteSpace(user.NickName) ? string.Empty : user.NickName.Replace("'", "''"),
                                                            DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")) + Environment.NewLine);

                tempQuery.Append(string.Format(@"delete from Users2Groups where UserID = {0} and GroupID = {1};", user.ID, group.ID) + Environment.NewLine);
                tempQuery.Append(string.Format(@"insert into Users2Groups(UserID, GroupID) values({0}, {1});", user.ID, group.ID) + Environment.NewLine);

                if (insertUsersQuery.Length + tempQuery.Length >= 524288)
                {
                    DataProvider.LogWithLogId("Scan of group with ID: " + group.ID + string.Format(" was finished. Writing the results... Progress: {0}%", GetPersentageString(usersCounter, usersInGroup.Count)), logId);

                    DataProvider.ExecuteNonQuery(insertUsersQuery.ToString());
                    insertUsersQuery.Clear();
                }

                insertUsersQuery.Append(tempQuery.ToString());
                usersCounter++;
            }

            DataProvider.ExecuteNonQuery(insertUsersQuery.ToString());

            DataProvider.ExecuteNonQuery("delete from Queue where ID = " + recordId);

            DataProvider.LogWithLogId("Scan of group with ID: " + group.ID + string.Format(" was finished. Writing the results... Progress: 100%"), logId);
            DataProvider.Log("Writing the results was finished. Group ID: " + group.ID + ", users in group: " + usersInGroup.Count);

            return;
        }

        private void ScanUser(string id, string recordId)
        {
            var accessToken = DataProvider.ExecuteScalar("select Value from Configuration where Name = 'AccessToken'").ToString();

            GroupInfoResponse groupInfoResponse = ExecuteRequest<GroupInfoResponse>("groups.getById", string.Format("group_id={0}&fields=city,country,description,members_count,status,verified,site", id), accessToken);

            if (groupInfoResponse.Groups.Length != 1)
            {
                DataProvider.ExecuteNonQuery("delete from Queue where ID = " + recordId);
                DataProvider.Log("Error getting group info for group ID: " + id);
            }
            else
            {
                Thread.Sleep(200);
                List<User> usersInGroup = new List<User>();

                for (var offset = 0; offset < groupInfoResponse.Groups[0].MembersCount; offset = offset + 1000)
                {
                    GroupGetMembersResponseWrapper groupGetMembersResponse = ExecuteRequest<GroupGetMembersResponseWrapper>("groups.getMembers",
                        string.Format("group_id={0}&fields=sex,bdate,city,country,site,status,last_seen,nickname,verified&offset={1}&count=1000", id, offset),
                        accessToken);

                    if (groupGetMembersResponse.Response.Users.Length > 0)
                    {
                        usersInGroup.AddRange(groupGetMembersResponse.Response.Users);
                    }

                    Thread.Sleep(1000);
                }

                RecordGroupScanResults(groupInfoResponse.Groups[0], usersInGroup, recordId);
            }
        }

        private T ExecuteRequest<T>(string method, string queryString, string accessToken) where T : class, new()
        {
            //var requestUrl = "https://api.vk.com/method/{0}?{1}&access_token={2}&lang={3}&v={4}";
            var requestUrl = "https://api.vk.com/method/{0}?{1}&lang={2}&v={3}";

            var responseObject = new T();
            var request = WebRequest.Create(string.Format(requestUrl, method, queryString, API_LANGUAGE, API_VERSION)) as HttpWebRequest;
            request.ContentType = "application/json;charset=utf-8";
            //request.TransferEncoding = "utf-8";

            using (var response = request.GetResponse() as HttpWebResponse)
            {
                if (response.StatusCode != HttpStatusCode.OK)
                    throw new Exception(String.Format(
                    "Server error (HTTP {0}: {1}).",
                    response.StatusCode,
                    response.StatusDescription));

                //var responseText = string.Empty;
                //using (var reader = new System.IO.StreamReader(response.GetResponseStream()))
                //{
                //    responseText = reader.ReadToEnd();
                //}

                responseObject =
                    new DataContractJsonSerializer(typeof(T)).ReadObject(
                    new MemoryStream(Encoding.UTF8.GetBytes(ReplaceNonCharacters(Encoding.UTF8.GetString(
                        ReadStream(response.GetResponseStream())), ' ')))) as T;
            }

            return responseObject;
        }

        private byte[] ReadStream(Stream input)
        {
            byte[] buffer = new byte[16 * 1024];
            using (MemoryStream ms = new MemoryStream())
            {
                int read;
                while ((read = input.Read(buffer, 0, buffer.Length)) > 0)
                {
                    ms.Write(buffer, 0, read);
                }
                return ms.ToArray();
            }
        }

        private string ReplaceNonCharacters(string aString, char replacement)
        {
            var sb = new StringBuilder(aString.Length);
            for (var i = 0; i < aString.Length; i++)
            {
                if (char.IsSurrogatePair(aString, i))
                {
                    int c = char.ConvertToUtf32(aString, i);
                    i++;
                    if (IsCharacter(c))
                        sb.Append(char.ConvertFromUtf32(c));
                    else
                        sb.Append(replacement);
                }
                else
                {
                    char c = aString[i];
                    if (IsCharacter(c))
                        sb.Append(c);
                    else
                        sb.Append(replacement);
                }
            }
            return sb.ToString();
        }

        private bool IsCharacter(int point)
        {
            return point < 0xFDD0 || // everything below here is fine
                point > 0xFDEF &&    // exclude the 0xFFD0...0xFDEF non-characters
                (point & 0xfffE) != 0xFFFE; // exclude all other non-characters
        }

        public class DbWorkerParams
        {
            public string GroupID { get; set; }
            public DataTable Users { get; set; }
            public List<string> UserIDs { get; set; }

            public DbWorkerParams()
            {
                UserIDs = new List<string>();
            }
        }

        [DataContract]
        private class Group
        {
            [DataMember(Name = "id")]
            public int ID { get; set; }
            [DataMember(Name = "name")]
            public string Name { get; set; }
            [DataMember(Name = "is_closed")]
            public int IsClosed { get; set; }
            [DataMember(Name = "deactivated")]
            public string Deactivated { get; set; }
            [DataMember(Name = "type")]
            public string Type { get; set; }
            [DataMember(Name = "city", EmitDefaultValue = true)]
            [DefaultValue("Uspecified")]
            public Place City { get; set; }
            [DataMember(Name = "country", EmitDefaultValue = true)]
            [DefaultValue("Uspecified")]
            public Place Country { get; set; }
            [DataMember(Name = "description")]
            public string Description { get; set; }
            [DataMember(Name = "members_count")]
            public int MembersCount { get; set; }
            [DataMember(Name = "status")]
            public string Status { get; set; }
            [DataMember(Name = "site")]
            public string Site { get; set; }
            [DataMember(Name = "verified")]
            public int Verified { get; set; }
            public DateTime ScanDate { get; set; }
        }

        [DataContract]
        private class User
        {
            [DataMember(Name = "id")]
            public int ID { get; set; }
            [DataMember(Name = "first_name")]
            public string FirstName { get; set; }
            [DataMember(Name = "last_name")]
            public string LastName { get; set; }
            [DataMember(Name = "verified")]
            public int Verified { get; set; }
            [DataMember(Name = "sex")]
            public int Sex { get; set; }
            [DataMember(Name = "bdate")]
            public string BDate { get; set; }
            [DataMember(Name = "city")]
            public Place City { get; set; }
            [DataMember(Name = "country")]
            public Place Country { get; set; }
            [DataMember(Name = "site")]
            public string Site { get; set; }
            [DataMember(Name = "status")]
            public string Status { get; set; }
            [DataMember(Name = "last_seen")]
            public LastSeen LastSeenInfo { get; set; }
            [DataMember(Name = "nickname")]
            public string NickName { get; set; }
            public DateTime ScanDate { get; set; }
        }

        [DataContract]
        private class LastSeen
        {
            [DataMember(Name = "time")]
            public int Time { get; set; }
            [DataMember(Name = "platform")]
            public int Platform { get; set; }

        }

        [DataContract]
        private class ErrorResponse
        {
            [DataMember(Name = "error")]
            public Error Response { get; set; }
        }

        [DataContract]
        private class Error
        {
            [DataMember(Name = "error_code")]
            public int ErrorCode { get; set; }
            [DataMember(Name = "error_msg")]
            public string ErrorMessage { get; set; }
        }

        [DataContract]
        private class Place
        {
            [DataMember(Name = "title")]
            public string Title { get; set; }
        }


        [DataContract]
        private class GroupInfoResponse
        {
            [DataMember(Name = "response")]
            public Group[] Groups { get; set; }
            [DataMember(Name = "error")]
            public Error Error { get; set; }
        }

        [DataContract]
        private class GroupGetMembersResponse
        {
            [DataMember(Name = "items")]
            public User[] Users { get; set; }
        }

        [DataContract]
        private class GroupGetMembersResponseWrapper
        {
            [DataMember(Name = "response")]
            public GroupGetMembersResponse Response { get; set; }
            [DataMember(Name = "error")]
            public Error Error { get; set; }
        }
    }
}