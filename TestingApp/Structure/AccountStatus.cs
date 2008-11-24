using System;
using Org.Reddragonit.Dbpro.Structure;
using FieldType = Org.Reddragonit.Dbpro.Structure.Attributes.Field.FieldType;

namespace TestingApp.Structure
{
	[Org.Reddragonit.Dbpro.Structure.Attributes.Table("ACCOUNT_STATUS")]
	public class AccountStatus : Table
	{
		private string _statusName;
		private long _statusId;
		//private byte[] _data;
		
		public AccountStatus()
		{
		}

        [Org.Reddragonit.Dbpro.Structure.Attributes.PrimaryKeyField("STATUS_ID", FieldType.LONG, false, true)]
        public long StatusId
        {
            get
            {
                return _statusId;
            }
            set{
                _statusId = value;
            }
        }

        [Org.Reddragonit.Dbpro.Structure.Attributes.Field("STATUS_NAME", FieldType.STRING, false, 50)]
        public string StatusName
        {
            get
            {
                return _statusName;
            }
            set
            {
                _statusName = value;
            }
        }

        /*[Org.Reddragonit.Dbpro.Structure.Attributes.Field("STATUS_BYTES", FieldType.BYTE,-1)]
        public byte[] Data
        {
            get
            {
                return _data;
            }
            set
            {
                _data = value;
            }
        }*/
	}
}
