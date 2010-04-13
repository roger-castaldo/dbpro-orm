using System;
using System.Collections.Generic;
using System.Text;
using System.Diagnostics;
using System.Threading;
using System.IO;
using NLog;

namespace Org.Reddragonit.Dbpro
{
    public class Logger
    {

        private static NLog.Logger lg = null; 

        public static void SetLogger(string name)
        {
            lg = LogManager.GetLogger(name);
        }

        public static void LogLine(object o)
        {
            System.Diagnostics.Debug.WriteLine(o);
            if (lg != null)
            {
                if (o is Exception)
                {
                    Exception e = (Exception)o;
                    while (e != null)
                    {
                        lg.Error(e.Message);
                        lg.Error(e.Source);
                        lg.Error(e.StackTrace);
                        e= e.InnerException;    
                    }
                }else
                    lg.Trace(o);
            }
        }

        public static void Log(object o)
        {
            System.Diagnostics.Debug.Write(o);
            if (lg != null)
            {
                if (o is Exception)
                {
                    Exception e = (Exception)o;
                    while (e != null)
                    {
                        lg.Error(e.Message);
                        lg.Error(e.Source);
                        lg.Error(e.StackTrace);
                        e = e.InnerException;
                    }
                }
                else
                    lg.Trace(o);
            }
        }
    }
}
