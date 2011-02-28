using Org.Reddragonit.Dbpro.Structure.Mapping;
using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Xml;
using Org.Reddragonit.Dbpro.Structure;

namespace Org.Reddragonit.Dbpro.Connections
{
    internal struct EnumTranslationPair
    {
        private string _origName;
        public string OriginalName
        {
            get { return _origName; }
        }

        private string _newName;
        public string NewName
        {
            get { return _newName; }
        }

        public EnumTranslationPair(string originalName, string newName)
        {
            _origName = originalName;
            _newName = newName;
        }
    }

	public static class ConnectionPoolManager
	{

        internal enum TriggerTypes{
            PRE_UPDATE,
            POST_UPDATE,
            PRE_DELETE,
            POST_DELETE,
            PRE_INSERT,
            POST_INSERT,
            PRE_DELETE_ALL,
            POST_DELETE_ALL
        }
		
		public readonly static string DEFAULT_CONNECTION_NAME="Org.Reddragonit.Dbpro.Connections.DEFAULT";
		private readonly static string CONNECTION_CONFIG_FILENAME="DbPro.xml";

		private static Dictionary<string, ConnectionPool> _connectionPools = new Dictionary<string, ConnectionPool>();
        private static Dictionary<Type, List<ITrigger>> _triggers = new Dictionary<Type, List<ITrigger>>();
        internal static Dictionary<Type, List<EnumTranslationPair>> _translations = new Dictionary<Type, List<EnumTranslationPair>>();
		
		static ConnectionPoolManager()
		{
			Utility.WaitOne(_connectionPools);
			string basePath=AppDomain.CurrentDomain.SetupInformation.ConfigurationFile.Substring(0,AppDomain.CurrentDomain.SetupInformation.ConfigurationFile.LastIndexOf(Path.DirectorySeparatorChar));
			FileInfo fi = RecurLocateConfigFile(new DirectoryInfo(basePath));
			if (fi!=null)
			{
				Logger.LogLine("Loaded config file: "+fi.FullName);
				XmlDocument doc = new XmlDocument();
				doc.Load(fi.OpenRead());
                foreach (XmlNode node in doc.DocumentElement.ChildNodes)
                {
                    if (node.Name == "ENUM_TRANSLATION")
                    {
                        Type t = Utility.LocateType(node["TYPE"].Value);
                        if (t!=null){
                            if (!_translations.ContainsKey(t))
                                _translations.Add(t, new List<EnumTranslationPair>());
                            _translations[t].Add(new EnumTranslationPair(node["ORIGINAL_NAME"].Value, node["NEW_NAME"].Value));
                        }
                    }
                }
				foreach (XmlNode node in doc.DocumentElement.ChildNodes)
				{
					if (node.Name=="DBConnection")
					{
						ExtractConnectionFromXml(node);
                    }
				}
			}
			string[] tmp = new string[_connectionPools.Count];
			_connectionPools.Keys.CopyTo(tmp,0);
			ClassMapper.CorrectConnectionNames(tmp);
			foreach (ConnectionPool pool in _connectionPools.Values)
			{
				pool.Init(_translations);
			}
			Utility.Release(_connectionPools);
		}
		
		public static void ReloadClasses(){
			Utility.WaitOne(_connectionPools);
			ClassMapper.ReInit();
			string[] tmp = new string[_connectionPools.Count];
			_connectionPools.Keys.CopyTo(tmp,0);
			ClassMapper.CorrectConnectionNames(tmp);
			foreach (ConnectionPool pool in _connectionPools.Values)
			{
				pool.Init(_translations);
			}
			Utility.Release(_connectionPools);
		}

        public static void RegisterTrigger(Type objectType, ITrigger trigger)
        {
            Monitor.Enter(_triggers);
            if (!_triggers.ContainsKey(objectType))
                _triggers.Add(objectType, new List<ITrigger>());
            _triggers[objectType].Add(trigger);
            Monitor.Exit(_triggers);
        }

        internal static void RunTriggers(Type tblType, TriggerTypes type)
        {
            ITrigger[] tmp = new ITrigger[0];
            Monitor.Enter(_triggers);
            if (_triggers.ContainsKey(tblType))
            {
                tmp = new ITrigger[_triggers[tblType].Count];
                _triggers[tblType].CopyTo(tmp);
            }
            Monitor.Exit(_triggers);
            foreach (ITrigger tr in tmp)
            {
                switch (type)
                {
                    case TriggerTypes.PRE_DELETE_ALL:
                        tr.PreDeleteAll();
                        break;
                    case TriggerTypes.POST_DELETE_ALL:
                        tr.PostDeleteAll();
                        break;
                }
            }
        }

        internal static void RunTriggers(Table original,Table tbl, TriggerTypes type)
        {
            ITrigger[] tmp = new ITrigger[0];
            Monitor.Enter(_triggers);
            if (_triggers.ContainsKey(tbl.GetType())){
                tmp = new ITrigger[_triggers[tbl.GetType()].Count];
                _triggers[tbl.GetType()].CopyTo(tmp);
            }
            Monitor.Exit(_triggers);
            foreach (ITrigger tr in tmp)
            {
                switch (type)
                {
                    case TriggerTypes.PRE_DELETE:
                        tr.PreDelete(tbl);
                        break;
                    case TriggerTypes.POST_DELETE:
                        tr.PostDelete(tbl);
                        break;
                    case TriggerTypes.PRE_INSERT:
                        tr.PreInsert(tbl);
                        break;
                    case TriggerTypes.POST_INSERT:
                        tr.PostInsert(tbl);
                        break;
                    case TriggerTypes.PRE_UPDATE:
                        tr.PreUpdate(original,tbl, tbl.ChangedFields);
                        break;
                    case TriggerTypes.POST_UPDATE:
                        tr.PostUpdate(original,tbl, tbl.ChangedFields);
                        break;
                    case TriggerTypes.PRE_DELETE_ALL:
                        tr.PreDeleteAll();
                        break;
                    case TriggerTypes.POST_DELETE_ALL:
                        tr.PostDeleteAll();
                        break;
                }
            }
        }
		
		private static void ExtractConnectionFromXml(XmlNode connectionNode)
		{
			Type t = Type.GetType(connectionNode.Attributes["connection_type"].Value,false);
			if (t!=null)
			{
				Dictionary<string, string> parameters = new Dictionary<string, string>();
				ConstructorInfo[] constructors=t.GetConstructors();
				foreach (XmlNode node in connectionNode.ChildNodes)
				{
					if (node.Name=="ConnectionParameter")
					{
						if (node.Attributes["parameter_value"].Value=="null")
						{
							parameters.Add(node.Attributes["parameter_name"].Value,null);
						}else{
							parameters.Add(node.Attributes["parameter_name"].Value,node.Attributes["parameter_value"].Value);
						}
					}
				}
				ConstructorInfo selected=null;
				int paramCount=0;
				foreach (ConstructorInfo c in constructors)
				{
					bool canWork=true;
					foreach (ParameterInfo p in c.GetParameters())
					{
						if (!parameters.ContainsKey(p.Name))
						{
							canWork=false;
							break;
						}
					}
					if (canWork)
					{
						if (paramCount<c.GetParameters().Length)
						{
							paramCount=c.GetParameters().Length;
							selected=c;
						}
					}
				}
				if (selected!=null)
				{
					object[] pars = new object[selected.GetParameters().Length];
					for (int x=0;x<selected.GetParameters().Length;x++)
					{
						ParameterInfo p = selected.GetParameters()[x];
						if (p.ParameterType==typeof(int))
							pars[x]=int.Parse(parameters[p.Name]);
						else if (p.ParameterType==typeof(long))
							pars[x]=long.Parse(parameters[p.Name]);
						else if (p.ParameterType==typeof(bool))
							pars[x]=bool.Parse(parameters[p.Name]);
						else
							pars[x]=parameters[p.Name];
					}
					selected.Invoke(pars);
				}else{
					throw new Exception("Unable to load connection "+t.FullName+".  Unable to find constructor with supplied parameters.");
				}
			}
		}
		
		private static FileInfo RecurLocateConfigFile(DirectoryInfo di)
		{
			foreach (FileInfo fi in di.GetFiles())
			{
				if (fi.Name.ToUpper()==CONNECTION_CONFIG_FILENAME.ToUpper())
					return fi;
			}
			foreach (DirectoryInfo d in di.GetDirectories())
			{
				FileInfo f = RecurLocateConfigFile(d);
				if (f!=null)
					return f;
			}
			return null;
		}
		
		public static ConnectionPool GetConnection(Type type)
		{
			TableMap tmp = ClassMapper.GetTableMap(type);
			if (tmp!=null)
				return GetConnection(tmp.ConnectionName);
			return null;
		}

		public static ConnectionPool GetConnection(string name)
		{
			ConnectionPool ret = null;
			Utility.WaitOne(_connectionPools);
			if (name==null)
			{
				name=DEFAULT_CONNECTION_NAME;
			}
			if (_connectionPools.ContainsKey(name))
			{
				ret =_connectionPools[name];
			}else 
				ret= _connectionPools[DEFAULT_CONNECTION_NAME];
			Utility.Release(_connectionPools);
			return ret;
		}

		internal static void AddConnection(string name, ConnectionPool pool)
		{
			if (name==null)
			{
				name =DEFAULT_CONNECTION_NAME;
			}
			if (_connectionPools.ContainsKey(name))
			{
				if (name==DEFAULT_CONNECTION_NAME)
				{
					if (!_connectionPools[name].Equals(pool))
					{
						throw new Exception("Cannot have more than one default connection pool.");
					}
				}else if (!_connectionPools[name].Equals(pool))
				{
					throw new Exception("Cannot replace a connection pool with a different one please change the name of the connection pool ("+name+")");
				}
				_connectionPools.Remove(name);
			}
			_connectionPools.Add(name, pool);
		}
		
		internal static bool ConnectionExists(string name)
		{
			bool ret=false;
			if (name==null)
				name=DEFAULT_CONNECTION_NAME;
			Utility.WaitOne(_connectionPools);
			ret=_connectionPools.ContainsKey(name);
			Utility.Release(_connectionPools);
			return ret;
		}

	}
}
