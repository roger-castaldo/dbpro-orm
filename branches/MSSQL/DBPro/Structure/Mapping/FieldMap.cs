/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 19/03/2008
 * Time: 9:40 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */

using System;
using System.Reflection;
using Org.Reddragonit.Dbpro.Structure.Attributes;

namespace Org.Reddragonit.Dbpro.Structure.Mapping
{
	/// <summary>
	/// Description of FieldMap.
	/// </summary>
	internal abstract class FieldMap
	{
		protected bool _primaryKey=false;
		protected bool _autogen=false;
		protected bool _nullable=false;
		protected bool _versionable=false;
		private bool _isArray = false;
		private Type _objectType=null;
		
		public FieldMap(PropertyInfo info)
		{
			_isArray = info.ToString().Contains("[]");
			_objectType=info.PropertyType;
			foreach (object obj in info.GetCustomAttributes(true))
			{
				if (obj is INullable)
				{
					_nullable=((INullable)obj).Nullable;
				}
				if (obj is ForeignField)
				{
					_nullable=((ForeignField)obj).Nullable;
				}
				if (obj is IPrimaryKeyField)
				{
					IPrimaryKeyField p = (IPrimaryKeyField)obj;
					_primaryKey=true;
					_autogen=p.AutoGen;
				}else if (obj is IVersionField)
				{
					_versionable=true;
				}
			}
			if (IsArray && PrimaryKey)
				throw new Exception("Unable to use an Array value as a Primary Key");
		}
		
		internal FieldMap(bool PrimaryKey,bool AutoGen,bool Nullable)
		{
			_primaryKey=PrimaryKey;
			_autogen=AutoGen;
			_nullable=Nullable;
		}
		
		public override bool Equals(object obj)
		{
			if ((obj==null)||!(obj is FieldMap))
				return false;
			FieldMap fm = (FieldMap)obj;
			return (fm.AutoGen==AutoGen)&&(fm.Nullable==Nullable)&&(fm.PrimaryKey==PrimaryKey)&&(fm.Versionable==Versionable);
		}
		
		public FieldMap(bool primaryKey, bool autogen, bool nullable,bool versionable)
		{
			this._primaryKey = primaryKey;
			this._autogen = autogen;
			this._nullable = nullable;
			this._versionable=versionable;
		}
		
		
		public bool PrimaryKey
		{
			get{
				return _primaryKey;
			}
		}
		
		public bool AutoGen{
			get{
				return _autogen;
			}
		}
		
		public bool Nullable{
			get{
				return _nullable;
			}
		}
		
		public bool Versionable
		{
			get{
				return _versionable;
			}
		}
		
		public bool IsArray{
			get
			{
				return _isArray;
			}
		}
		
		public Type ObjectType{
			get{return _objectType;}
		}
	}
}
