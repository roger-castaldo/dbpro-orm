using System;
using System.Collections.Generic;
using System.Text;
using System.Reflection;

namespace Org.Reddragonit.Dbpro.Logging
{
    public interface ILogWriter
    {
        void LogMessage(Assembly assembly, LogLevel level, object entry);
    }
}
