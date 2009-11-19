using System;
using System.Collections.Generic;
using System.Text;
using System.Diagnostics;
using System.Threading;
using System.IO;

namespace Org.Reddragonit.Dbpro
{
    public class Logger : TraceListener
    {

        private static Mutex mut = new Mutex(false);
        private static List<Logger> _loggers;

        public static void AddLogger(string path)
        {
            mut.WaitOne();
            if (_loggers == null)
                _loggers = new List<Logger>();
            _loggers.Add(new Logger(path));
            mut.ReleaseMutex();
        }

        public static void LogLine(object o)
        {
            System.Diagnostics.Debug.WriteLine(o);
            if (_loggers != null)
            {
                foreach (Logger l in _loggers)
                    l.WriteLine(o);
            }
        }

        public static void Log(object o)
        {
            System.Diagnostics.Debug.Write(o);
            if (_loggers != null)
            {
                foreach (Logger l in _loggers)
                    l.Write(o);
            }
        }

        delegate void ContentWriteDelegate(object content);

        private string _path;
        private string _curFileName="";
        private Mutex mutNewFile = new Mutex(false);
        private TextWriter tw=null;
        private ContentWriteDelegate _delWriteContent;
        private ContentWriteDelegate _delWriteContentLine;

        private Logger(string path)
        {
            _path = path;
            _delWriteContent = new ContentWriteDelegate(WriteContent);
            _delWriteContentLine = new ContentWriteDelegate(WriteContentLine);
            if (_path.EndsWith(Path.DirectorySeparatorChar.ToString()))
                _path = _path.Substring(0, _path.Length - 1);
            if (!new DirectoryInfo(_path).Exists)
                new DirectoryInfo(_path).Create();
        }

        private string CurrentFileName
        {
            get { return DateTime.Now.ToString("yyyy_MM_dd") + ".txt"; }
        }

        private void WriteContent(object content)
        {
            mutNewFile.WaitOne();
            if (_curFileName != CurrentFileName)
            {
                _curFileName = CurrentFileName;
                if (tw!=null){
                    tw.Flush();
                    tw.Close();
                }
                tw = new StreamWriter(new FileStream(_path + Path.DirectorySeparatorChar + CurrentFileName, FileMode.Create, FileAccess.Write, FileShare.Read));
            }
            tw.Write(DateTime.Now.ToString("HH:mm:ss-->"));
            tw.Write(content);
            tw.Flush();
            mutNewFile.ReleaseMutex();
        }

        public override void Write(object o)
        {
            _delWriteContent.BeginInvoke(o, null, null);
        }

        public override void Write(object o, string category)
        {
            _delWriteContent.BeginInvoke(o, null, null);
        }

        public override void Write(string message)
        {
            _delWriteContent.BeginInvoke(message, null, null);
        }

        public override void Write(string message, string category)
        {
            _delWriteContent.BeginInvoke(message, null, null);
        }

        private void WriteContentLine(object content)
        {
            mutNewFile.WaitOne();
            if (_curFileName != CurrentFileName)
            {
                _curFileName = CurrentFileName;
                if (tw != null)
                {
                    tw.Flush();
                    tw.Close();
                }
                tw = new StreamWriter(new FileStream(_path + Path.DirectorySeparatorChar + CurrentFileName,FileMode.Create, FileAccess.Write, FileShare.Read));
            }
            tw.Write(DateTime.Now.ToString("HH:mm:ss-->"));
            tw.WriteLine(content);
            tw.Flush();
            mutNewFile.ReleaseMutex();
        }

        public override void WriteLine(object o)
        {
            _delWriteContentLine.BeginInvoke(o, null, null);
        }

        public override void WriteLine(object o, string category)
        {
            _delWriteContentLine.BeginInvoke(o, null, null);
        }

        public override void WriteLine(string message)
        {
            _delWriteContentLine.BeginInvoke(message, null, null);
        }

        public override void WriteLine(string message, string category)
        {
            _delWriteContentLine.BeginInvoke(message, null, null);
        }

    }
}
