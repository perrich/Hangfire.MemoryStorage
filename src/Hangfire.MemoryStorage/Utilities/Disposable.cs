using System;

namespace Hangfire.MemoryStorage.Utilities
{
    public class Disposable : IDisposable
    {
        public static Disposable Create(Action onDispose)
        {
            return new Disposable { OnDispose = onDispose };
        }

        private Disposable() { }
        
        private Action OnDispose { get; set; }
        
        public void Dispose()
        {
            OnDispose();
        }
    }
}