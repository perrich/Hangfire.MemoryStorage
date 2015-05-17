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
            var dict = GetDictionary(typeof (T));
            return dict.Values.Cast<T>();
        }

        public static IEnumerable<IExpirable> GetExpirables(Type type)
        {
            if (!type.IsAssignableFrom(typeof (IExpirable)))
                return new List<IExpirable>();

            var dict = GetDictionary(type);
            return (IEnumerable<IExpirable>) dict.Values;
        }

        public static T GetOrCreate<T>(string key, T element)
        {
            var dict = GetDictionary(typeof (T));
            return (T) dict.GetOrAdd(key, element);
        }

        public static T Get<T>(string key) where T : class
        {
            object obj = null;
            var dict = GetDictionary(typeof (T));
            dict.TryGetValue(key, out obj);
            return (T) obj;
        }

        public static T Get<T>(int key) where T : class
        {
            object obj = null;
            var dict = GetDictionary(typeof (T));
            dict.TryGetValue(key, out obj);
            return (T) obj;
        }

        internal static void Create<T>(IEnumerable<IIntIdentifiedData> elements)
        {
            var dict = GetDictionary(typeof (T));
            foreach (var element in elements)
            {
                dict.TryAdd(element.Id, element);
            }
        }

        public static T Create<T>(IIntIdentifiedData element)
        {
            var dict = GetDictionary(typeof (T));
            dict.TryAdd(element.Id, element);

            return (T) element;
        }

        public static T Create<T>(IStringIdentifiedData element)
        {
            var dict = GetDictionary(typeof (T));
            dict.TryAdd(element.Id, element);

            return (T) element;
        }

        public static void Delete(Type type, IIntIdentifiedData element)
        {
            var dict = GetDictionary(type);
            dict.Remove(element.Id);
        }

        public static void Delete(Type type, IEnumerable<IIntIdentifiedData> elements)
        {
            var dict = GetDictionary(type);

            foreach (var element in elements)
            {
                dict.Remove(element.Id);
            }
        }

        public static void Delete(Type type, IStringIdentifiedData element)
        {
            var dict = GetDictionary(type);
            dict.Remove(element.Id);
        }

        public static void Delete(Type type, IEnumerable<IStringIdentifiedData> elements)
        {
            var dict = GetDictionary(type);

            foreach (var element in elements)
            {
                dict.Remove(element.Id);
            }
        }

        private static ConcurrentDictionary<object, object> GetDictionary(Type type)
        {
            return Dictionary.GetOrAdd(type, x => new ConcurrentDictionary<object, object>());
        }
    }
}