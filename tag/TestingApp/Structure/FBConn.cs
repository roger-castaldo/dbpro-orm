using System;
using Org.Reddragonit.Dbpro.Connections;

namespace TestingApp.Structure
{
	
	[Org.Reddragonit.Dbpro.Connections.Attributes.Connection("FBMain")]
	public class FBConn : Connection
	{
		public FBConn()
		{
		}
	}
}
