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
using UpdateDeleteAction =  Org.Reddragonit.Dbpro.Structure.Attributes.ForiegnField.UpdateDeleteAction;
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
        private bool _isArray = false; 
		
		public ExternalFieldMap(System.Type type,MemberInfo info) : base(info)
		{
			_type=type;
            _isArray = info.ToString().Contains("[]");
			foreach (object obj in info.GetCustomAttributes(true))
			{
				if (obj is IForiegnField)
				{
					IForiegnField f = (IForiegnField)obj;
					_onUpdate=f.OnUpdate;
					_onDelete=f.OnDelete;
				}
			}
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

        public bool IsArray{
            get
            {
                return _isArray;
            }
        }
		
		public System.Type Type{
			get{
				return _type;
			}
		}
	}
}
