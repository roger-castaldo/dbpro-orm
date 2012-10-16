/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 21/03/2008
 * Time: 11:35 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */

using System;
using System.Reflection;
using UpdateDeleteAction =  Org.Reddragonit.Dbpro.Structure.Attributes.ForeignField.UpdateDeleteAction;
using Org.Reddragonit.Dbpro.Structure.Attributes;

namespace Org.Reddragonit.Dbpro.Structure.Mapping
{
	/// <summary>
	/// Description of ExternalFieldMap.
	/// </summary>
	internal class ExternalFieldMap : FieldMap
	{
		
		private System.Type _type;
		private UpdateDeleteAction _onUpdate;
		private UpdateDeleteAction _onDelete;
		private bool _selfRelated=false;
		private string _addonName=null;
		
		public ExternalFieldMap(System.Type type,bool isSelfRelated,PropertyInfo info) : base(info)
		{
			_type=type;
			_selfRelated=isSelfRelated;
			_addonName="";
			if (info.Name.ToUpper() != info.Name)
			{
				foreach (char c in info.Name.ToCharArray())
				{
					if (c.ToString().ToUpper() == c.ToString())
					{
						_addonName += "_" + c.ToString().ToUpper();
					}
					else
					{
						_addonName += c.ToString().ToUpper();
					}
				}
			}else{
				_addonName=info.Name;
			}
			if (_addonName.StartsWith("_"))
				_addonName=_addonName.Substring(1);
			foreach (object obj in info.GetCustomAttributes(true))
			{
				if (obj is IForeignField)
				{
					IForeignField f = (IForeignField)obj;
					_onUpdate=f.OnUpdate;
					_onDelete=f.OnDelete;
				}
				if (obj is IVersionField)
				{
					_versionable=true;
				}
			}
		}
		
		public override bool Equals(object obj)
		{
			if ((obj==null)||!(obj is ExternalFieldMap))
				return false;
			ExternalFieldMap efm = (ExternalFieldMap)obj;
			return base.Equals(obj)&&(efm.Type==Type)&&(efm.OnUpdate==OnUpdate)&&(efm.OnDelete==OnDelete)&&(efm.IsArray==IsArray);
		}
		
		public UpdateDeleteAction OnUpdate{
			get{
				return _onUpdate;
			}
		}
		
		public UpdateDeleteAction OnDelete{
			get{
				return _onDelete;
			}
		}
		
		public bool IsSelfRelated{
			get{return _selfRelated;}
		}
		
		public string AddOnName{
			get{return _addonName;}
		}
		
		public System.Type Type{
			get{
				return _type;
			}
		}
	}
}
