/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 22/01/2009
 * Time: 12:54 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using Org.Reddragonit.Dbpro.Connections.Parameters;
using System;
using System.Collections.Generic;
using System.Security;
using System.Security.Cryptography;
using MersenneTwister;
using Org.Reddragonit.Dbpro.Connections;
using Org.Reddragonit.Dbpro.Structure.Attributes;

namespace TestingApp.Structure
{
	
	public enum UserTypes{
		Normal,
		Admin,
		ReadOnly
	}
	
	/// <summary>
	/// Description of User.
	/// </summary>
    [Table()]
	public class User : Org.Reddragonit.Dbpro.Structure.Table 
	{
		public enum PasswordSecurityType
		{
			ENCRYTPED=0,
			HASHED=1,
			PLAIN=2
		}
		
		public enum HashAlgorythm
		{
			MD5=0,
			SHA256=1,
			SHA512=2
		}
		
		public enum EncryptionAlgorythm
		{
			DES=0,
			TRIPLE_DES=1,
			RIJNDAEL=2
		}
		
		private static readonly string PASS_SECURITY_TYPE_KEY="Org.Reddragonit.Core.Users.PasswordSecurityType";
		private static readonly string PASS_HASH_TYPE_KEY="Org.Reddragonit.Core.Users.HashAlgorythm";
		private static readonly string PASS_ENCRYPTION_TYPE_KEY = "Org.Reddragonit.Core.Users.EncryptionAlgorythm";
		private static readonly string USER_MANAGEMENT_CONNECTION_KEY = "Org.Reddragonit.Core.Users.CONNECTION_NAME";
		
		private static PasswordSecurityType _securityType= PasswordSecurityType.PLAIN;
		private static HashAlgorythm _hashAlgorythm = HashAlgorythm.SHA512;
		private static EncryptionAlgorythm _encryptionAlgorythm = EncryptionAlgorythm.RIJNDAEL;
		private static string _connectionName;
		
		static User()
		{
			/*if (ConfigurationSettings.AppSettings[USER_MANAGEMENT_CONNECTION_KEY]==null)
			{
				throw new Exception("Unable to load User Management System.  Requires: "+USER_MANAGEMENT_CONNECTION_KEY+" in application settings.");
			}
			_connectionName=ConfigurationSettings.AppSettings[USER_MANAGEMENT_CONNECTION_KEY];
			if (ConfigurationSettings.AppSettings[PASS_SECURITY_TYPE_KEY]==null)
			{
				throw new Exception("Unable to load User Management System.  Requires: "+PASS_SECURITY_TYPE_KEY+" in the application settings.");
			}
			_securityType=(PasswordSecurityType)Enum.Parse(typeof(PasswordSecurityType),ConfigurationSettings.AppSettings[PASS_SECURITY_TYPE_KEY]);
			if (_securityType== PasswordSecurityType.ENCRYTPED)
			{
				if (ConfigurationSettings.AppSettings[PASS_SECURITY_TYPE_KEY]==null)
				{
					throw new Exception("Unable to load User Management System.  Requires: "+PASS_ENCRYPTION_TYPE_KEY+" in the application settings when Security Type set to ENCRYPTED");
				}
				_encryptionAlgorythm =(EncryptionAlgorythm)Enum.Parse(typeof(EncryptionAlgorythm),ConfigurationSettings.AppSettings[PASS_SECURITY_TYPE_KEY]);
			}else if (_securityType==PasswordSecurityType.HASHED)
			{
				if (ConfigurationSettings.AppSettings[PASS_HASH_TYPE_KEY]==null)
				{
					throw new Exception("Unable to load User Management System. Requires: "+PASS_HASH_TYPE_KEY+" in the application settings when Security Type set to ENCRYPTED");
				}
				_hashAlgorythm= (HashAlgorythm)Enum.Parse(typeof(HashAlgorythm),ConfigurationSettings.AppSettings[PASS_HASH_TYPE_KEY]);
			}*/
		}
		
		public PasswordSecurityType SecurityType
		{
			get{return _securityType;}
		}
		
		public EncryptionAlgorythm EncryptionType
		{
			get{return _encryptionAlgorythm;}
		}
		
		public HashAlgorythm HashType
		{
			get{return _hashAlgorythm;}
		}
		
		[PrimaryKeyField(true)]
		public long ID
		{
			get{ return (long)get(); }
		}
		
		[Field(100,false)]
		public string FirstName
		{
			get{ return (string)get();}
			set{ set(value); }
		}
		
		[Field(150,false)]
		public string LastName
		{
			get{ return (string)get(); }
			set{set(value);}
		}
		
		[Field(200,false)]
		public string UserName
		{
			get{return (string)get();}
			set{set(value);}
		}
		
		[VersionField("passwrd",FieldType.STRING,false,5000,VersionField.VersionTypes.NUMBER)]
		internal string InternalPassword
		{
			get{ return (string)get();}
			set{set(value);}
		}
		
		[Field("Active",FieldType.BOOLEAN,false)]
		public bool Active
		{
			get{return (bool)get();}
			set{set(value);}
		}
		
		[Field(FieldType.ENUM)]
		public UserTypes Type{
			get{return (UserTypes)get();}
			set{set(value);}
		}
		
		public string Password
		{
			get{return InternalPassword;}
			set{
				if (_seedIsNull)
				{
					MT19937 m = new MT19937();
					Seed=m.genrand_int31();
					_seedIsNull=false;
				}
				InternalPassword=SecurePassword(value);
			}
		}
		
		[ForeignField(ForeignField.UpdateDeleteAction.CASCADE,ForeignField.UpdateDeleteAction.CASCADE)]
		public SecurityRight[] Rights
		{
			get{ return (SecurityRight[])get();}
			set{ set(value); }
		}
		
		[ForeignField(false,ForeignField.UpdateDeleteAction.CASCADE,ForeignField.UpdateDeleteAction.CASCADE)]
		public Group UserGroup
		{
			get{return (Group)get();}
			set{set(value);}
		}
		
		public bool HasRight(SecurityRight right)
		{
			return HasRight(right.Name);
		}
		
		public bool HasRight(string rightName)
		{
			if (Rights!=null)
			{
				foreach (SecurityRight right in Rights)
				{
					if (right.Name==rightName)
						return true;
				}
			}
			if (UserGroup!=null)
			{
				return 	UserGroup.HasRight(rightName);
			}
			return false;
		}
		
		private bool IsPasswordMatch(string password)
		{
			return SecurePassword(password)==InternalPassword;
		}
		
		private string SecurePassword(string password)
		{
			int minBlock=int.MaxValue;
			if (SecurityType== PasswordSecurityType.ENCRYTPED)
			{
				int keylen=0;
				ICryptoTransform stream=null;
				SymmetricAlgorithm crypt;
				switch (EncryptionType)
				{
					case EncryptionAlgorythm.DES:
						crypt = DESCryptoServiceProvider.Create();
						break;
					case EncryptionAlgorythm.TRIPLE_DES:
						crypt = TripleDESCryptoServiceProvider.Create();
						break;
					default:
						crypt=new RijndaelManaged();
						break;
				}
				crypt.Mode=CipherMode.CBC;
				for (int x=0;x<crypt.LegalKeySizes.Length;x++)
				{
					if (keylen<crypt.LegalKeySizes[x].MaxSize)
					{
						keylen=crypt.LegalKeySizes[x].MaxSize;
					}
				}
				for (int x=0;x<crypt.LegalBlockSizes.Length;x++)
				{
					if (minBlock>crypt.LegalBlockSizes[x].MinSize)
					{
						minBlock=crypt.LegalBlockSizes[x].MinSize;
					}
				}
				crypt.Key=GenerateKey(keylen);
				stream=crypt.CreateEncryptor();
				byte[] pass = PadPasswordByte(password,minBlock);
				password=Convert.ToBase64String(stream.TransformFinalBlock(pass,0,pass.Length));
			}else if(SecurityType== PasswordSecurityType.HASHED)
			{
				HashAlgorithm hash = null;
				switch(HashType)
				{
					case HashAlgorythm.MD5:
						minBlock=256;
						hash=MD5CryptoServiceProvider.Create();
						break;
					case HashAlgorythm.SHA256:
						minBlock=512;
						hash=new SHA256Managed();
						break;
					case HashAlgorythm.SHA512:
						minBlock=1024;
						hash=new SHA512Managed();
						break;
				}
				password=Convert.ToBase64String(hash.ComputeHash(PadPasswordByte(password,minBlock)));
			}
			return password;
		}
		
		private byte[] PadPasswordByte(string password,int blockSize)
		{
			MT19937 m = new MT19937(Seed);
			while (password.Length%blockSize!=0)
			{
				password+=(char)System.Convert.ToByte(m.genrand_int31());
			}
			byte[] ret = new byte[password.Length];
			for (int x=0;x<ret.Length;x++)
			{
				ret[x]=(byte)password[x];
			}
			return ret;
		}
		
		private byte[] GenerateKey(int keyLen)
		{
			return new MT19937(Seed).genrand_bytearray(keyLen);
		}
		
		[Field(false)]
		internal long Seed
		{
			get{return (long)get();}
			set{set(value);}
		}
		
		[Field(FieldType.BYTE)]
		internal byte SecuritySettings
		{
			get{
                byte? ret = (byte?)get();
				if (!ret.HasValue)
				{
					ret=0;
					ret|=(byte)(((byte)SecurityType)<<6);
					ret|=(byte)(((byte)EncryptionType)<<4);
					ret|=(byte)(((byte)HashType)<<2);
                    SecuritySettings = ret.Value;
				}
				return (byte)get();
			}
			set{set(value);}
		}
		
		[Field(FieldType.BOOLEAN)]
		internal bool LoggedIn
		{
			get{return (bool)get();}
			set{set(value);}
		}
		
		public bool IsLoggedIn
		{
			get{return LoggedIn;}
		}
		
		private static Connection conn
		{
			get{
                return ConnectionPoolManager.GetConnection(typeof(User));
			}
		}
		
		private bool _seedIsNull=true;
		
		public User()
		{
		}
		
		public static User Instance()
		{
            return new User();
		}
		
		internal static User Save(User user)
		{
			Connection c = conn;
			User ret = (User)c.Save((Org.Reddragonit.Dbpro.Structure.Table)user);
			c.Commit();
			c.CloseConnection();
			return ret;
		}
		
		public static List<User> LoadAllUsers()
		{
			Connection c = conn;
			List<User> ret = new List<User>();
			List<Org.Reddragonit.Dbpro.Structure.Table> tbls = c.SelectAll(typeof(User));
			foreach (Org.Reddragonit.Dbpro.Structure.Table tbl in tbls)
				ret.Add((User)tbl);
			c.CloseConnection();
			return ret;
		}
		
		internal static User LoginUser(string username,string password)
		{
			List<Org.Reddragonit.Dbpro.Structure.Table> users=new List<Org.Reddragonit.Dbpro.Structure.Table>();
			Connection c = conn;
			List<SelectParameter> pars = new List<SelectParameter>();
            pars.Add(new EqualParameter("UserName",username ));
            users.AddRange(c.Select(typeof(User),pars).ToArray());
            c.CloseConnection();
            foreach (User u in users)
            {
            	if (u.IsPasswordMatch(password)&&u.Active)
            		return u;
            }
            return null;
		}
	}
}
