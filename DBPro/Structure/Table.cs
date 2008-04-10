using System;
using System.Reflection;
using System.Collections.Generic;
using System.Threading;
using Org.Reddragonit.Dbpro.Structure.Mapping;
using FieldNamePair = Org.Reddragonit.Dbpro.Structure.Mapping.TableMap.FieldNamePair;

namespace Org.Reddragonit.Dbpro.Structure
{
	public abstract class Table
	{
		private List<string> _nullFields=null;
        internal bool _isSaved = false;
        private Dictionary<string, object> _initialPrimaryValues = null; 
		
		public Table()
		{
			ClassMapper.GetTableMap(this.GetType());
            _nullFields = new List<String>();
		}

        internal bool IsSaved
        {
            get
            {
                return _isSaved;
            }
        }
		
		internal void SetValues(Org.Reddragonit.Dbpro.Connections.Connection conn)
		{
            _initialPrimaryValues = new Dictionary<string, object>();
            TableMap map = ClassMapper.GetTableMap(this.GetType());
			foreach (FieldNamePair fnp in map.FieldNamePairs)
			{
                if (this.GetType().GetProperty(fnp.ClassFieldName).PropertyType.IsSubclassOf(typeof(Table)))
                {
                    Table t = (Table)this.GetType().GetProperty(fnp.ClassFieldName).PropertyType.GetConstructor(Type.EmptyTypes).Invoke(new object[0]);
                    t.SetValues(conn);
                    if (!t.AllFieldsNull){
                        this.GetType().GetProperty(fnp.ClassFieldName).SetValue(this, t, new object[0]);
                    }
                    else{
                        _nullFields.Add(fnp.ClassFieldName);
                    }
                }
                else
                {
                    if (conn.IsDBNull(conn.GetOrdinal(fnp.TableFieldName)))
                    {
                        if (_nullFields == null)
                        {
                            _nullFields = new List<string>();
                        }
                        _nullFields.Add(fnp.ClassFieldName);
                    }
                    else
                    {
                        this.GetType().GetProperty(fnp.ClassFieldName).SetValue(this, conn[fnp.TableFieldName], new object[0]);
                    }
                }
                if (map[fnp].PrimaryKey || !map.HasPrimaryKeys )
                {
                    _initialPrimaryValues.Add(fnp.ClassFieldName, GetField(fnp.ClassFieldName));
                }
			}
            _isSaved = true;
		}

        internal bool AllFieldsNull
        {
            get
            {
                if (_nullFields.Count == ClassMapper.GetTableMap(this.GetType()).FieldNamePairs.Count)
                {
                    return true;
                }
                return false;
            }
        }
		
		protected object GetField(string FieldName)
		{
			if (IsFieldNull(FieldName))
			{
				return null;
			}else
			{
				return this.GetType().GetProperty(FieldName).GetValue(this,new object[0]);
			}
		}

        internal object GetInitialPrimaryValue(FieldNamePair pair)
        {
            if (_initialPrimaryValues.ContainsKey(pair.ClassFieldName))
            {
                return _initialPrimaryValues[pair.ClassFieldName];
            }
            return null;
        }

		protected bool IsFieldNull(string FieldName)
		{
			if (_nullFields==null)
			{
				return false;
			}
			foreach (string str in _nullFields)
			{
				if (str==FieldName)
				{
					return true;
				}
			}
			return false;
		}
		
	}
}
