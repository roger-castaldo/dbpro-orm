/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 28/10/2008
 * Time: 11:18 AM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */

using System;

namespace Org.Reddragonit.Dbpro.Connections.Firebird
{
	/// <summary>
	/// Description of FBQueryBuilder.
	/// </summary>
	internal class FBQueryBuilder : QueryBuilder 
	{
		public FBQueryBuilder()
		{
		}
		
		protected override string CreateNullConstraintString {
			get { return "UPDATE RDB$RELATION_FIELDS SET RDB$NULL_FLAG = 1 WHERE RDB$FIELD_NAME = '{1}' AND RDB$RELATION_NAME = '{0}'"; }
		}
	}
}
