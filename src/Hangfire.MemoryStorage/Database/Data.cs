using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Hangfire.MemoryStorage.Dto;
using Hangfire.MemoryStorage.Utilities;

namespace Hangfire.MemoryStorage.Database
{
    public class Data
    {
        private readonly ConcurrentDictionary<Type, ConcurrentDictionary<object, object>> Dictionary =
            new ConcurrentDictionary<Type, ConcurrentDictionary<object, object>>();

        public IEnumerable<T> GetEnumeration<T>()
        {
            var dict = GetDictionary(typeof(T));
            return dict.Values.Cast<T>();
        }

        public ICollection<object> GetEnumeration(Type type)
        {
            var dict = GetDictionary(type);
            return dict.Values;
        }

        public T GetOrCreate<T,K>(K key, T element) where T : IIdentifiedData<K>
        {
            var dict = GetDictionary(typeof(T));
            return (T)dict.GetOrAdd(key, element);
        }

        public T Get<T>(string key) where T : IIdentifiedData<string>
        {
            object obj = null;
            var dict = GetDictionary(typeof(T));
            dict.TryGetValue(key, out obj);
            return (T)obj;
        }

        public T Get<T>(int key) where T : IIdentifiedData<int>
        {
            object obj = null;
            var dict = GetDictionary(typeof(T));
            dict.TryGetValue(key, out obj);
            return (T)obj;
        }

        internal void Create<K>(IEnumerable<IIdentifiedData<K>> elements)
        {
            if (elements.Any())
            {
                var dict = GetDictionary(elements.First().GetType());
                foreach (var element in elements)
                {
                    dict.TryAdd(element.Id, element);
                }
            }
        }

        public void Create<K>(IIdentifiedData<K> element)
        {
            var dict = GetDictionary(element.GetType());
            dict.TryAdd(element.Id, element);
        }

        public void Delete<K>(IIdentifiedData<K> element)
        {
            var dict = GetDictionary(element.GetType());
            dict.Remove(element.Id);
        }

        public void Delete<K>(IEnumerable<IIdentifiedData<K>> elements)
        {
            if (elements.Any())
            {
                var dict = GetDictionary(elements.First().GetType());

                foreach (var element in elements)
                {
                    dict.Remove(element.Id);
                }
            }
        }

        private ConcurrentDictionary<object, object> GetDictionary(Type type)
        {
            return Dictionary.GetOrAdd(type, x => new ConcurrentDictionary<object, object>());
        }
    }
}