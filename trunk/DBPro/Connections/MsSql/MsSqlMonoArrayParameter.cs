using System;
using System.Collections.Generic;
using System.Text;
using System.Data;

namespace Org.Reddragonit.Dbpro.Connections.MsSql
{
    internal class MsSqlMonoArrayParameter : IDbDataParameter
    {
        public MsSqlMonoArrayParameter(string name, object value)
        {
            _name = name;
            _value = value;
        }

        #region IDbDataParameter Members

        public byte Precision
        {
            get
            {
                return 0;
            }
            set
            { 
            }
        }

        public byte Scale
        {
            get
            {
                return 0;
            }
            set
            {
            }
        }

        public int Size
        {
            get
            {
                return 0;
            }
            set
            {
            }
        }

        #endregion

        #region IDataParameter Members

        public DbType DbType
        {
            get
            {
                return DbType.Binary;
            }
            set
            {
                
            }
        }

        public ParameterDirection Direction
        {
            get
            {
                return ParameterDirection.Input;
            }
            set
            {
            }
        }

        public bool IsNullable
        {
            get { return true; }
        }

        private string _name;
        public string ParameterName
        {
            get
            {
                return _name;
            }
            set
            {
                _name = value;
            }
        }

        public string SourceColumn
        {
            get
            {
                return null;
            }
            set
            {
            }
        }

        public DataRowVersion SourceVersion
        {
            get
            {
                return DataRowVersion.Current;
            }
            set
            {
            }
        }

        private object _value;
        public object Value
        {
            get
            {
                return _value;
            }
            set
            {
                _value = value;
            }
        }

        #endregion
    }
}
