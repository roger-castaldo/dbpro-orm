using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Xml;
using Org.Reddragonit.Dbpro.Structure;
using Org.Reddragonit.Dbpro.Connections.Parameters;

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

        public delegate void delPreInit(ConnectionPool pool);
        public delegate void delPostInit(ConnectionPool pool);
        public delegate void delGetAdditionalsForTable(Type tableType,out List<View> views, out List<StoredProcedure> procedures, out List<Type> additionalTypes,out List<Trigger> triggers);
		
		internal readonly static string DEFAULT_CONNECTION_NAME="Org.Reddragonit.Dbpro.Connections.DEFAULT";
		private readonly static string CONNECTION_CONFIG_FILENAME="DbPro.xml";

		private static Dictionary<string, ConnectionPool> _connectionPools = new Dictionary<string, ConnectionPool>();
        private static Dictionary<Type, List<ITrigger>> _triggers = new Dictionary<Type, List<ITrigger>>();
        internal static Dictionary<Type, List<EnumTranslationPair>> _translations = new Dictionary<Type, List<EnumTranslationPair>>();
        private static Dictionary<string, List<delPreInit>> _preInits = new Dictionary<string,List<delPreInit>>();
        private static Dictionary<string, List<delPostInit>> _postInits = new Dictionary<string,List<delPostInit>>();
        private static Dictionary<string, List<delGetAdditionalsForTable>> _getAdditionals = new Dictionary<string,List<delGetAdditionalsForTable>>();
        private static List<string> _poolsInitted = new List<string>();
		
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
            foreach (Type t in Utility.LocateTypeInstances(typeof(ITrigger)))
            {
                foreach (TriggerRegisterAttribute tra in t.GetCustomAttributes(typeof(TriggerRegisterAttribute), false))
                    RegisterTrigger(tra.Table, (ITrigger)t.GetConstructor(Type.EmptyTypes).Invoke(new object[0]));
            }
			Utility.Release(_connectionPools);
		}

        public static void AssemblyAdded()
        {
            Utility.WaitOne(_connectionPools);
            foreach (string str in _connectionPools.Keys)
                _connectionPools[str].AssemblyAdded();
            Utility.Release(_connectionPools);
        }

        public static void RegisterPreInitDelegate(string poolName, delPreInit preInit)
        {
            Utility.WaitOne(_preInits);
            poolName = (poolName == null ? DEFAULT_CONNECTION_NAME : poolName);
            List<delPreInit> inits = new List<delPreInit>();
            if (_preInits.ContainsKey(poolName))
            {
                inits = _preInits[poolName];
                _preInits.Remove(poolName);
            }
            inits.Add(preInit);
            _preInits.Add(poolName, inits);
            Utility.Release(_preInits);
        }

        public static void UnRegisterPreInitDelegate(string poolName, delPreInit preInit)
        {
            Utility.WaitOne(_preInits);
            poolName = (poolName == null ? DEFAULT_CONNECTION_NAME : poolName);
            List<delPreInit> inits = new List<delPreInit>();
            if (_preInits.ContainsKey(poolName))
            {
                inits = _preInits[poolName];
                _preInits.Remove(poolName);
            }
            inits.Remove(preInit);
            _preInits.Add(poolName, inits);
            Utility.Release(_preInits);
        }

        public static void RegisterPostInitDelegate(string poolName, delPostInit postInit)
        {
            Utility.WaitOne(_postInits);
            poolName = (poolName == null ? DEFAULT_CONNECTION_NAME : poolName);
            List<delPostInit> inits = new List<delPostInit>();
            if (_postInits.ContainsKey(poolName))
            {
                inits = _postInits[poolName];
                _postInits.Remove(poolName);
            }
            inits.Add(postInit);
            _postInits.Add(poolName, inits);
            Utility.Release(_postInits);
        }

        public static void UnRegisterPostInitDelegate(string poolName, delPostInit postInit)
        {
            Utility.WaitOne(_postInits);
            poolName = (poolName == null ? DEFAULT_CONNECTION_NAME : poolName);
            List<delPostInit> inits = new List<delPostInit>();
            if (_postInits.ContainsKey(poolName))
            {
                inits = _postInits[poolName];
                _postInits.Remove(poolName);
            }
            inits.Remove(postInit);
            _postInits.Add(poolName, inits);
            Utility.Release(_postInits);
        }

        public static void RegisterAdditionalsForTable(string poolName, delGetAdditionalsForTable additionals)
        {
            Utility.WaitOne(_getAdditionals);
            poolName = (poolName == null ? DEFAULT_CONNECTION_NAME : poolName);
            List<delGetAdditionalsForTable> adds = new List<delGetAdditionalsForTable>();
            if (_getAdditionals.ContainsKey(poolName))
            {
                adds = _getAdditionals[poolName];
                _getAdditionals.Remove(poolName);
            }
            adds.Add(additionals);
            _getAdditionals.Add(poolName, adds);
            Utility.Release(_getAdditionals);
        }

        public static void UnRegisterAdditionalsForTable(string poolName, delGetAdditionalsForTable additionals)
        {
            Utility.WaitOne(_getAdditionals);
            poolName = (poolName == null ? DEFAULT_CONNECTION_NAME : poolName);
            List<delGetAdditionalsForTable> adds = new List<delGetAdditionalsForTable>();
            if (_getAdditionals.ContainsKey(poolName))
            {
                adds = _getAdditionals[poolName];
                _getAdditionals.Remove(poolName);
            }
            adds.Remove(additionals);
            _getAdditionals.Add(poolName, adds);
            Utility.Release(_getAdditionals);
        }

        public static void RegisterTrigger(Type objectType, ITrigger trigger)
        {
            Monitor.Enter(_triggers);
            if (!_triggers.ContainsKey(objectType))
                _triggers.Add(objectType, new List<ITrigger>());
            _triggers[objectType].Add(trigger);
            Monitor.Exit(_triggers);
        }

        internal static void RunTriggers(Connection conn, Type tblType, TriggerTypes type, out bool abort)
        {
            abort = false;
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
                        tr.PreDeleteAll(conn,out abort);
                        break;
                    case TriggerTypes.POST_DELETE_ALL:
                        tr.PostDeleteAll(conn);
                        break;
                }
            }
        }

        internal static void RunTriggers(Connection conn, Type tableType, SelectParameter[] parameters, TriggerTypes type, out bool abort)
        {
            abort = false;
            ITrigger[] tmp = new ITrigger[0];
            Monitor.Enter(_triggers);
            if (_triggers.ContainsKey(tableType))
            {
                tmp = new ITrigger[_triggers[tableType].Count];
                _triggers[tableType].CopyTo(tmp);
            }
            Monitor.Exit(_triggers);
            foreach (ITrigger tr in tmp)
            {
                switch (type)
                {
                    case TriggerTypes.PRE_DELETE:
                        tr.PreDelete(conn,tableType, parameters,out abort);
                        break;
                    case TriggerTypes.POST_DELETE:
                        tr.PostDelete(conn,tableType, parameters);
                        break;
                }
            }
        }

        internal static void RunTriggers(Connection conn, Type tableType, Dictionary<string, object> updateFields, SelectParameter[] parameters, TriggerTypes type, out bool abort)
        {
            abort = false;
            ITrigger[] tmp = new ITrigger[0];
            Monitor.Enter(_triggers);
            if (_triggers.ContainsKey(tableType))
            {
                tmp = new ITrigger[_triggers[tableType].Count];
                _triggers[tableType].CopyTo(tmp);
            }
            Monitor.Exit(_triggers);
            foreach (ITrigger tr in tmp)
            {
                switch (type)
                {
                    case TriggerTypes.PRE_UPDATE:
                        tr.PreUpdate(conn,tableType, updateFields, parameters,out abort);
                        break;
                    case TriggerTypes.POST_UPDATE:
                        tr.PostUpdate(conn,tableType, updateFields, parameters);
                        break;
                }
            }
        }

        internal static void RunTriggers(Connection conn, Table original, Table tbl, TriggerTypes type, out bool abort)
        {
            abort = false;
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
                        tr.PreDelete(conn,tbl,out abort);
                        break;
                    case TriggerTypes.POST_DELETE:
                        tr.PostDelete(conn, tbl);
                        break;
                    case TriggerTypes.PRE_INSERT:
                        tr.PreInsert(conn, tbl, out abort);
                        break;
                    case TriggerTypes.POST_INSERT:
                        tr.PostInsert(conn, tbl);
                        break;
                    case TriggerTypes.PRE_UPDATE:
                        tr.PreUpdate(conn, original, tbl, tbl.ChangedFields, out abort);
                        break;
                    case TriggerTypes.POST_UPDATE:
                        tr.PostUpdate(conn, original, tbl, tbl.ChangedFields);
                        break;
                    case TriggerTypes.PRE_DELETE_ALL:
                        tr.PreDeleteAll(conn, out abort);
                        break;
                    case TriggerTypes.POST_DELETE_ALL:
                        tr.PostDeleteAll(conn);
                        break;
                }
            }
        }
		
		private static void ExtractConnectionFromXml(XmlNode connectionNode)
		{
			Type t = Type.GetType(connectionNode.Attributes["connection_type"].Value,false);
            if (t != null)
                t.GetConstructor(new Type[] { typeof(XmlElement) }).Invoke(new object[] { connectionNode });
            else
                throw new Exception("Unable to load connection of type " + connectionNode.Attributes["connection_type"].Value);
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
		
		public static ConnectionPool GetPool(Type type)
		{
            if (type.GetCustomAttributes(typeof(Org.Reddragonit.Dbpro.Structure.Attributes.Table), false).Length > 0)
            {
                Org.Reddragonit.Dbpro.Structure.Attributes.Table t = (Org.Reddragonit.Dbpro.Structure.Attributes.Table)type.GetCustomAttributes(typeof(Org.Reddragonit.Dbpro.Structure.Attributes.Table), false)[0];
                return GetPool(t.ConnectionName);
            }
            else if (type.GetCustomAttributes(typeof(Org.Reddragonit.Dbpro.Virtual.ClassViewAttribute), false).Length > 0)
            {
                Org.Reddragonit.Dbpro.Virtual.ClassViewAttribute cva = (Org.Reddragonit.Dbpro.Virtual.ClassViewAttribute)type.GetCustomAttributes(typeof(Org.Reddragonit.Dbpro.Virtual.ClassViewAttribute), false)[0];
                return GetPool(cva.ConnectionName);
            }
			return null;
		}

		public static ConnectionPool GetPool(string name)
		{
			ConnectionPool ret = null;
			Utility.WaitOne(_connectionPools);
            name = (name == null ? DEFAULT_CONNECTION_NAME : name);
			if (_connectionPools.ContainsKey(name))
				ret =_connectionPools[name];
			else 
				ret= _connectionPools[DEFAULT_CONNECTION_NAME];
            if (!_poolsInitted.Contains(ret.ConnectionName))
            {
                Utility.WaitOne(_preInits);
                if (_preInits.ContainsKey(name))
                {
                    foreach (delPreInit del in _preInits[name])
                        del.Invoke(ret);
                }
                Utility.Release(_preInits);
                ret.Init(_translations);
                _poolsInitted.Add(ret.ConnectionName);
                Utility.WaitOne(_postInits);
                if (_postInits.ContainsKey(ret.ConnectionName))
                {
                    foreach (delPostInit del in _postInits[name])
                        del.Invoke(ret);
                }
                Utility.Release(_postInits);
            }
			Utility.Release(_connectionPools);
			return ret;
		}

        public static Connection GetConnection(Type type)
        {
            ConnectionPool pool = GetPool(type);
            return (pool == null ? null : pool.GetConnection());
        }

        public static Connection GetConnection(string name){
            ConnectionPool pool = GetPool(name);
            return (pool == null ? null : pool.GetConnection());
        }

		internal static void AddConnection(string name, ConnectionPool pool)
		{
            name = (name == null ? DEFAULT_CONNECTION_NAME : name);
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
            name = (name == null ? DEFAULT_CONNECTION_NAME : name);
			Utility.WaitOne(_connectionPools);
			ret=_connectionPools.ContainsKey(name);
			Utility.Release(_connectionPools);
			return ret;
		}

        internal static void GetAdditionalsForTable(ConnectionPool pool,Type type, out List<View> views, out List<StoredProcedure> procedures, out List<Type> types, out List<Trigger> triggers)
        {
            views = new List<View>();
            procedures = new List<StoredProcedure>();
            types = new List<Type>();
            triggers = new List<Trigger>();
            Utility.WaitOne(_getAdditionals);
            if (_getAdditionals.ContainsKey(pool.ConnectionName))
            {
                foreach (delGetAdditionalsForTable del in _getAdditionals[pool.ConnectionName])
                {
                    List<View> tviews;
                    List<StoredProcedure> tprocedures;
                    List<Type> ttypes;
                    List<Trigger> ttriggers;
                    del.Invoke(type, out tviews, out tprocedures, out ttypes,out ttriggers);
                    views.AddRange(tviews);
                    procedures.AddRange(tprocedures);
                    types.AddRange(ttypes);
                    triggers.AddRange(ttriggers);
                }
            }
            Utility.Release(_getAdditionals);
        }
	}
}
