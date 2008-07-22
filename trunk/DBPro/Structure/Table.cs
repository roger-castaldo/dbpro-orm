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
        private Dictionary<string, object> _initialValues = null;

		public Table()
		{
			ClassMapper.GetTableMap(this.GetType());
            _nullFields = new List<String>();
            _initialPrimaryValues = new Dictionary<string, object>();
            _initialValues = new Dictionary<string, object>();
            TableMap map = ClassMapper.GetTableMap(this.GetType());
            foreach (FieldNamePair fnp in map.FieldNamePairs)
            {
                if (map[fnp].PrimaryKey)
                {
                    _initialPrimaryValues.Add(fnp.ClassFieldName, this.GetType().GetProperty(fnp.ClassFieldName).GetValue(this, new object[0]));
                }
                _initialValues.Add(fnp.ClassFieldName, this.GetType().GetProperty(fnp.ClassFieldName).GetValue(this, new object[0]));
            }
		}
		
		internal string ConnectionName
		{
			get{
				foreach (Attribute att in this.GetType().GetCustomAttributes(true))
				{
					if (att.GetType()==typeof(Org.Reddragonit.Dbpro.Structure.Attributes.Table))
					{
						return ((Org.Reddragonit.Dbpro.Structure.Attributes.Table)att).ConnectionName;
					}
				}
				return null;
			}
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
                if (map[fnp] is ExternalFieldMap)
                {
                    if (!((ExternalFieldMap)map[fnp]).IsArray)
                    {
                        Table t = (Table)this.GetType().GetProperty(fnp.ClassFieldName).PropertyType.GetConstructor(Type.EmptyTypes).Invoke(new object[0]);
                        t.SetValues(conn);
                        if (!t.AllFieldsNull)
                        {
                            this.GetType().GetProperty(fnp.ClassFieldName).SetValue(this, t, new object[0]);
                        }
                        else
                        {
                            _nullFields.Add(fnp.ClassFieldName);
                        }
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
                if (map[fnp].PrimaryKey || !map.HasPrimaryKeys)
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
                foreach (FieldNamePair fnp in ClassMapper.GetTableMap(this.GetType()).FieldNamePairs)
                {
                    if (!IsFieldNull(fnp.ClassFieldName))
                    {
                        return false;
                    }
                }
                return true;
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

        private bool equalObjects(object obj1, object obj2)
        {
            if (obj1 == null)
            {
                if (obj2 == null)
                {
                    return true;
                }
                return false;
            }
            else
            {
                if (obj2 == null)
                {
                    return false;
                }
                if (!(obj2.GetType().Equals(obj1.GetType())))
                {
                    return false;
                }
                try
                {
                    return obj1.Equals(obj2);
                }
                catch (Exception e)
                {
                    return false; 
                }
            }
        }

		internal bool IsFieldNull(string FieldName)
		{
            return equalObjects(_initialValues[FieldName], this.GetType().GetProperty(FieldName).GetValue(this, new object[0]));
		}
		
	}
}
