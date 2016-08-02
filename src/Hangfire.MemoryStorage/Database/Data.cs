using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Hangfire.MemoryStorage.Dto;
using Hangfire.MemoryStorage.Utilities;

namespace Hangfire.MemoryStorage.Database
{
    public static class Data
    {
        private static readonly ConcurrentDictionary<Type, ConcurrentDictionary<object, object>> Dictionary =
            new ConcurrentDictionary<Type, ConcurrentDictionary<object, object>>();

        public static IEnumerable<T> GetEnumeration<T>()
        {
            var dict = GetDictionary(typeof(T));
            return dict.Values.Cast<T>();
        }

        public static ICollection<object> GetEnumeration(Type type)
        {
            var dict = GetDictionary(type);
            return dict.Values;
        }

        public static T GetOrCreate<T,K>(K key, T element) where T : IIdentifiedData<K>
        {
            var dict = GetDictionary(typeof(T));
            return (T)dict.GetOrAdd(key, element);
        }

        public static T Get<T>(string key) where T : IIdentifiedData<string>
        {
            object obj = null;
            var dict = GetDictionary(typeof(T));
            dict.TryGetValue(key, out obj);
            return (T)obj;
        }

        public static T Get<T>(int key) where T : IIdentifiedData<int>
        {
            object obj = null;
            var dict = GetDictionary(typeof(T));
            dict.TryGetValue(key, out obj);
            return (T)obj;
        }

        internal static void Create<K>(IEnumerable<IIdentifiedData<K>> elements)
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

        public static void Create<K>(IIdentifiedData<K> element)
        {
            var dict = GetDictionary(element.GetType());
            dict.TryAdd(element.Id, element);
        }

        public static void Delete<K>(IIdentifiedData<K> element)
        {
            var dict = GetDictionary(element.GetType());
            dict.Remove(element.Id);
        }

        public static void Delete<K>(IEnumerable<IIdentifiedData<K>> elements)
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

        private static ConcurrentDictionary<object, object> GetDictionary(Type type)
        {
            return Dictionary.GetOrAdd(type, x => new ConcurrentDictionary<object, object>());
        }
    }
}