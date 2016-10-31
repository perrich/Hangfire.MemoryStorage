using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Storage.Monitoring;

namespace Hangfire.MemoryStorage.Utilities
{
    public class StateHistoryList : IList<StateHistoryDto>
    {
        private readonly IList<StateHistoryDto> _list = new List<StateHistoryDto>();

        public IEnumerator<StateHistoryDto> GetEnumerator()
        {
            return _list.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Add(StateHistoryDto item)
        {
            if (_list.Any(t => t.StateName == "Created"))
            {
                return;
            }

            _list.Add(item);
        }

        public void Clear()
        {
            _list.Clear();
        }

        public bool Contains(StateHistoryDto item)
        {
            return _list.Contains(item);
        }

        public void CopyTo(StateHistoryDto[] array, int arrayIndex)
        {
            _list.CopyTo(array, arrayIndex);
        }

        public bool Remove(StateHistoryDto item)
        {
            return _list.Remove(item);
        }

        public int Count
        {
            get { return _list.Count; }
        }

        public bool IsReadOnly
        {
            get { return _list.IsReadOnly; }
        }

        public int IndexOf(StateHistoryDto item)
        {
            return _list.IndexOf(item);
        }

        public void Insert(int index, StateHistoryDto item)
        {
            _list.Insert(index, item);
        }

        public void RemoveAt(int index)
        {
            _list.RemoveAt(index);
        }

        public StateHistoryDto this[int index]
        {
            get { return _list[index]; }
            set { _list[index] = value; }
        }
    }
}
