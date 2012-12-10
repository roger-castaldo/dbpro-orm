using System;
using System.Collections.Generic;
using System.Text;
using System.Diagnostics;
using System.Threading;
using System.IO;
using Org.Reddragonit.Dbpro.Logging;
using System.Reflection;

namespace Org.Reddragonit.Dbpro
{
    internal class Logger
    {

        private static object _lock = new object();

        private static List<ILogWriter> _writers;
        private static List<ILogWriter> Writers
        {
            get
            {
                Monitor.Enter(_lock);
                if (_writers == null)
                {
                    Type parent = typeof(ILogWriter);
                    _writers = new List<ILogWriter>();
                    foreach (Assembly ass in AppDomain.CurrentDomain.GetAssemblies())
                    {
                        try
                        {
                            if (ass.GetName().Name!="mscorlib" && !ass.GetName().Name.StartsWith("System.") && ass.GetName().Name!="System" && !ass.GetName().Name.StartsWith("Microsoft"))
                            {
                                foreach (Type t in ass.GetTypes())
                                {
                                    if (t.IsSubclassOf(parent) || (parent.IsInterface && new List<Type>(t.GetInterfaces()).Contains(parent)))
                                        _writers.Add((ILogWriter)t.GetConstructor(Type.EmptyTypes).Invoke(new object[0]));
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            if (e.Message != "The invoked member is not supported in a dynamic assembly.")
                            {
                                throw e;
                            }
                        }
                    }
                }
                Monitor.Exit(_lock);
                return _writers;
            }
        }

        public static void LogLine(object o)
        {
            Assembly ass = typeof(Logger).Assembly;
            if (o is Exception)
            {
                foreach (ILogWriter ilw in Writers)
                {
                    Exception e = (Exception)o;
                    while (e != null)
                    {
                        ilw.LogMessage(ass, LogLevel.Database, e);
                        ilw.LogMessage(ass, LogLevel.Critical, e);
                        e = e.InnerException;
                    }
                }
            }
            else
            {
                foreach (ILogWriter ilw in Writers)
                {
                    ilw.LogMessage(ass, LogLevel.Database, o);
                }
            }
        }
    }
}
