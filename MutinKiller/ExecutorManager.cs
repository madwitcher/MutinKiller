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
using MySql.Data.MySqlClient;
using System.Diagnostics;

namespace MutinKiller
{
    public class ExecutorManager
    {
        private static readonly object _lock = new object();

        private const string API_VERSION = "5.21";
        private const string API_LANGUAGE = "en";
        private const string EXECUTOR_THREAD = "EXECUTOR_THREAD";

        internal enum ExecutorStatus
        {
            Killed = 0,
            Alive = 1,
            Stopping = 2,
            Unknown = 4
        }

        private static bool m_stopRequested = false;

        internal static ExecutorStatus Status
        {
            get
            {
                var status = ExecutorStatus.Unknown;

                var executorThreadReference = HttpContext.Current.Application[EXECUTOR_THREAD] as Thread;

                if (executorThreadReference == null)
                    return ExecutorStatus.Killed;

                switch (executorThreadReference.ThreadState)
                {
                    case System.Threading.ThreadState.Suspended:
                    case System.Threading.ThreadState.Aborted:
                    case System.Threading.ThreadState.Stopped:
                        status = ExecutorStatus.Killed;
                        break;
                    case System.Threading.ThreadState.AbortRequested:
                    case System.Threading.ThreadState.StopRequested:
                    case System.Threading.ThreadState.SuspendRequested:
                        status = ExecutorStatus.Stopping;
                        break;
                    case System.Threading.ThreadState.Running:
                    case System.Threading.ThreadState.WaitSleepJoin:
                    case System.Threading.ThreadState.Background:
                        status = ExecutorStatus.Alive;
                        break;

                    default:
                        status = ExecutorStatus.Unknown;
                        break;
                }

                return status;
            }
        }

        internal static void Restart()
        {
            StopExecutor();
            Start();
        }

        internal static void Start()
        {
            try
            {
                DataProvider.Log("Attempting to start the executor...");
                if (IsExecutorAlive())
                {
                    DataProvider.Log("Executor is alive, no more instances is allowed.");
                    return;
                }
                Process executorProcess = new Process();
                executorProcess.StartInfo = new ProcessStartInfo
                {
                    CreateNoWindow = true,
                    FileName = HttpContext.Current.Server.MapPath("~/bin/") + "SimpleServer.exe",
                    WindowStyle = ProcessWindowStyle.Hidden,
                    UseShellExecute = false
                };
                executorProcess.Exited += new EventHandler(executorProcess_Exited);

                executorProcess.Start();
            }
            catch (Exception ex)
            {
                DataProvider.Log("Executor was not started. Details: " + ex.Message);
            }

            return;
        }

        static void executorProcess_Exited(object sender, EventArgs e)
        {
            DataProvider.Log("Executor was aborted by the system.");
        }

        internal static bool IsExecutorAlive()
        {
            Process[] processList = Process.GetProcessesByName("SimpleServer");
            return processList != null && processList.Length != 0;
        }

        internal static void StopExecutor()
        {
            DataProvider.Log("Attempting to stop the executor...");
            if (!IsExecutorAlive())
            {
                DataProvider.Log("Executor is dead, stop cannot be performed.");
            }
            else
            {
                foreach (Process process in Process.GetProcessesByName("SimpleServer"))
                {
                    process.Kill();
                    DataProvider.Log("Executor was killed.");
                }
            }
        }
    }
}