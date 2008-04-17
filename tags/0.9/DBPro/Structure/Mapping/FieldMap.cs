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
		
		public FieldMap(MemberInfo info)
		{
			foreach (object obj in info.GetCustomAttributes(true))
			{
				if (obj is IPrimaryKeyField)
				{
					IPrimaryKeyField p = (IPrimaryKeyField)obj;
					_primaryKey=true;
					_autogen=p.AutoGen;
					_nullable=p.Nullable;
				}
			}
		}
		
		public FieldMap(bool primaryKey, bool autogen, bool nullable)
		{
			this._primaryKey = primaryKey;
			this._autogen = autogen;
			this._nullable = nullable;
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
	}
}
