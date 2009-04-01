using System;
using System.Collections.Generic;
using System.Text;
using System.Collections;
using Org.Reddragonit.Dbpro.Connections;

namespace Org.Reddragonit.Dbpro.Structure
{
	public class PagedTableList : IEnumerable 
    {

        private int? _pageSize = 20;
        public int? PageSize
        {
            get { return _pageSize; }
            set { _pageSize = value; }
        }

        private SelectParameter[] _pars;
        public SelectParameter[] SelectParams
        {
            get { return _pars; }
        }
        
        private Type _tableType;
        public Type TableType{
        	get{return _tableType;}
        }
        
        private List<Table> _data;
        private int _count=0;
        private Connection _conn;

        public PagedTableList(Type type,int? PageSize,SelectParameter[] SelectParams)
        {
        	if (!type.IsSubclassOf(typeof(Table)))
        		throw new Exception("cannot produce a Paged Table List from a class that does not inherit Table");
        	_tableType=type;
            _pageSize = PageSize;
            _pars = SelectParams;
            _conn = ConnectionPoolManager.GetConnection(type).getConnection();
            _count = (int)_conn.SelectCount(type,_pars);
            if (!_pageSize.HasValue)
            	_pageSize=20;
            _data = new List<Table>();
            LoadToIndex(_pageSize.Value);
        }
        
        private void LoadToIndex(int index)
        {
        	while((_data.Count-1<index)&&(_data.Count<Count))
        	{
        		_data.AddRange(_conn.SelectPaged(TableType,SelectParams,(ulong)_data.Count,(ulong)PageSize));
        	}
        }
        
        
        #region IList<Table>
            	
		public Table this[int index]
		{
			get {
				if (_data.Count<index)
					LoadToIndex(index);
				return _data[index];
			}
			set {
				throw new Exception("Paged Table List is ReadOnly.");
			}
		}
    	
		public int Count {
			get {
				return _count;
			}
		}
    	
		public bool IsReadOnly {
			get {
				return true;
			}
		}
    	
		public int IndexOf(Table item)
		{
			LoadToIndex(Count+1);
			return _data.IndexOf(item);
		}
    	
		public void Insert(int index, Table item)
		{
			throw new Exception("Paged Table List is ReadOnly.");
		}
    	
		public void RemoveAt(int index)
		{
			throw new Exception("Paged Table List is ReadOnly.");
		}
    	
		public void Add(Table item)
		{
			throw new Exception("Paged Table List is ReadOnly.");
		}
    	
		public void Clear()
		{
			throw new Exception("Paged Table List is ReadOnly.");
		}
    	
		public bool Contains(Table item)
		{
			LoadToIndex(Count+1);
			return _data.Contains(item);
		}
    	
		public void CopyTo(Table[] array, int arrayIndex)
		{
			LoadToIndex(Count+1);
			_data.CopyTo(array,arrayIndex);
		}
    	
		public bool Remove(Table item)
		{
			throw new Exception("Paged Table List is ReadOnly.");
		}
    	
		IEnumerator IEnumerable.GetEnumerator()
		{
			return new PagedTableListEnumerator(this);
		}
		
		public PagedTableListEnumerator GetEnumerator()
		{
			return new PagedTableListEnumerator(this);
		}
		
		#endregion
    }
}
