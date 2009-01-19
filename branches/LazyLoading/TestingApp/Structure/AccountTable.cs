using System;
using Org.Reddragonit.Dbpro.Structure;
using FieldType = Org.Reddragonit.Dbpro.Structure.Attributes.FieldType;
using UpdateDeleteAction = Org.Reddragonit.Dbpro.Structure.Attributes.ForeignField.UpdateDeleteAction;

namespace TestingApp.Structure
{
	[Org.Reddragonit.Dbpro.Structure.Attributes.Table("ACCOUNTS")]
	public class AccountTable : Table
	{

		private long _id=0;
		private string _fName;
		private string _lName;
		private AccountStatus[] _status;
        private string _job;
		
		public AccountTable()
		{
			_status = new AccountStatus[0];
		}

		[Org.Reddragonit.Dbpro.Structure.Attributes.PrimaryKeyField("ACCOUNT_ID", FieldType.LONG, false, true)]
		public long Id {
			get { return _id; }
			set { _id = value; }
		}
		
		[Org.Reddragonit.Dbpro.Structure.Attributes.Field("FIRST_NAME", FieldType.STRING, false, 50)]
		public string FirstName {
			get { return _fName; }
			set { _fName = value; }
		}
		
		[Org.Reddragonit.Dbpro.Structure.Attributes.Field("LAST_NAME", FieldType.STRING, false, 50)]
		public string LastName {
			get { return _lName; }
			set { _lName = value; }
		}
		
		[Org.Reddragonit.Dbpro.Structure.Attributes.ForeignField(true,UpdateDeleteAction.CASCADE,UpdateDeleteAction.CASCADE)]
		public AccountStatus[] Status {
			get { return _status; }
			set { _status = value; }
		}

		[Org.Reddragonit.Dbpro.Structure.Attributes.Field()]
        public string Job
        {
            get { return _job; }
            set { _job = value; }
        }
		
	}
}
