using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using System.Collections;
using System.Collections.Concurrent;
using System.Reflection;
using System.Threading;

namespace BFelger
{
	/// <summary>
	/// ObjectPool manages objects whose instantiation and initialization costs
	/// prohibit constant allocation/deallocation. ObjectPool recycles old
	/// objects and dynamically increases and decreases the number of 
	/// allocated objects as demand requires.
	/// 
	/// USE: using (PoolAcquisition<OdbcConnection> connection = Source.Connections.AcquireDisposable() { }
	/// </summary>
	/// <typeparam name="T">Any type with the new() constraint.</typeparam>
	internal class ObjectPool<T> : IEnumerable
		where T : new()
	{
		#region IEnumerable Members

		public IEnumerator GetEnumerator()
		{
			return items.GetEnumerator();
		}

		#endregion

		#region Private Members

		// The ConcurrentDictionary allows us complete random access,
		// which we need for cleaning out old items
		ConcurrentDictionary<T, PoolItem<T>> items;

		#endregion

		#region Public Properties

		public int Count
		{
			get { return items.Count; }
		}

		public int MinItems
		{
			get;
			protected set;
		}

		public int MaxItems
		{
			get;
			protected set;
		}

		public int ItemLifeSpanMinutes
		{
			get;
			protected set;
		}

		public uint ItemsAllocated
		{
			get;
			protected set;
		}

		// This is not a proper property, but a public field.
		// As an atomic data type, it is completely threadsafe.
		// If I made a property out of it, I would have to lock
		// on it.
		public bool ShuttingDown;

		#endregion

		#region Public Constructors and Destructors

		/// <summary>
		/// ObjectPool constructor.
		/// </summary>
		/// <param name="MinItems">Minimum number of items in pool. The constructor will create these items, calling OnCreateHandler if supplied.</param>
		/// <param name="MaxItems">Maximum number of items in pool. Acquire() will return default values when this threshold is reached.</param>
		/// <param name="ItemLifeSpanMinutes">The longest an item should exist. Items that exceed their lifespan are removed and cleaned up.</param>
		/// <param name="OnCreateHandler">Event handler to call when a new item is created for the pool.</param>
		/// <param name="OnCleanUpHandler">Event handler to call when an expired item is removed from the pool.</param>
		public ObjectPool(int MinItems, int MaxItems, int ItemLifeSpanMinutes, OnCreateDelegate OnCreateHandler = null, OnCleanUpDelegate OnCleanUpHandler = null, OnAcquireDelegate OnAcquireHandler = null)
		{
			ShuttingDown = false;

			items = new ConcurrentDictionary<T, PoolItem<T>>();

			OnCreate = OnCreateHandler;
			OnCleanUp = OnCleanUpHandler;
			OnAcquire = OnAcquireHandler;

			this.MinItems = MinItems;
			this.MaxItems = MaxItems;
			this.ItemLifeSpanMinutes = ItemLifeSpanMinutes;
			
			for (int i = 0; i < MinItems; i++)
			{
				T newItem = new T();

				if (OnCreate != null)
				{
					OnCreate(ref newItem);
				}

				items[newItem] = new PoolItem<T>(newItem);
				ItemsAllocated++;
			}
		}

		/// <summary>
		/// Destructor. Cleans up items left in the pool, if necessary.
		/// </summary>
		~ObjectPool()
		{
			foreach (KeyValuePair<T, PoolItem<T>> keyValPair in items)
			{
				Release(keyValPair.Value.item);
			}
		}

		#endregion

		#region Callback Delegates

		public delegate void OnCreateDelegate(ref T newItem);
		public delegate void OnCleanUpDelegate(ref T oldItem);
		public delegate void OnAcquireDelegate(ref T item);

		private OnCreateDelegate OnCreate = null;
		private OnCleanUpDelegate OnCleanUp = null;
		private OnAcquireDelegate OnAcquire = null;

		#endregion

		#region Public Methods

		/// <summary>
		/// Get a new pool item. The pool will keep a count of this item, so it needs to be 
		/// given back to the pool with Release().
		/// </summary>
		/// <returns>A pool item, or default value if at maximum pool size.</returns>
		private T Acquire()
		{
			// If we're shutting down the pool, return the default. Since code calling Acquire
			// should be check for this anyway, this is kosher.
			if (ShuttingDown)
				return default(T);

			lock (items)
			{
				T returnItem = default(T);

				// First, check the recycle bin
				if (items.Count > 0)
				{
					// Order item in objectBag is now ordered by time created.
					IEnumerable<KeyValuePair<T, PoolItem<T>>> query = items.OrderByDescending(item => item.Value.timeCreated);

					// Trim the fat. Remove items from the tail that are past their lifespan.
					foreach (KeyValuePair<T, PoolItem<T>> keyValPair in query)
					{
						// Grab the first one and remember it. We'll pop it later.
						if (returnItem == null || returnItem.Equals(default(T)))
						{
							PoolItem<T> firstItem;
							if (items.TryRemove(keyValPair.Value.item, out firstItem))
								returnItem = firstItem.item;
						}
						else
						{
							// Check for items past their lifespan.
							// But only if we are above the minimum number of items.
							if (ItemsAllocated <= MinItems)
								break;

							// Is the current item past its lifespan?
							if (DateTime.Now.Subtract(keyValPair.Value.timeCreated).TotalMinutes > ItemLifeSpanMinutes)
							{
								// Remove the item, call cleanup on it, and reduce the item count.
								PoolItem<T> removedItem;
								if (items.TryRemove(keyValPair.Value.item, out removedItem))
								{
									if (OnCleanUp != null)
										OnCleanUp(ref removedItem.item);
									ItemsAllocated--;
								}
							}
						}
					}
				}

				// If we still don't have an object, then create a new one 
				if ((returnItem == null || returnItem.Equals(default(T))) && ItemsAllocated < MaxItems)
				{
					returnItem = new T();

					if (OnCreate != null)
					{
						OnCreate(ref returnItem);
					}

					// Notice we didn't add it to the pool. This is because it's
					// acquired as soon as it's created. We DO, however, remember
					// that we created it.
					ItemsAllocated++;
				}

				if (OnAcquire != null)
				{
					OnAcquire(ref returnItem);
				}

				// If this is still default(T) at this point, then all items are checked out,
				// and no more can be made. Return the default.
				return returnItem;
			}
		}

        public PoolAcquisition<T> AcquireDisposable()
        {
            return new PoolAcquisition<T>(Acquire(), this);
        }

		/// <summary>
		/// Releases the specified pool item, putting it back into the pool
		/// of available items for recycling.
		/// </summary>
		/// <param name="item">Item to recycle back into the pool.</param>
		public void Release(T item)
		{
			lock (items)
			{
				// We don't touch ItemsAllocated because it's already keeping track of it.
				items[item] = new PoolItem<T>(item);
			}
		}

		/// <summary>
		/// Prepares the pool for shutdown, and calls Release on each pool item.
		/// Does not account for extant items in use.
		/// </summary>
		public void Shutdown()
		{
			ShuttingDown = true;

			lock (items)
			{
				foreach (KeyValuePair<T, PoolItem<T>> keyValPair in items)
				{
					Release(keyValPair.Value.item);
				}
			}
		}

		/// <summary>
		/// In the event that items pulled from the queue are no longer useable, and 
		/// should not be repooled, Forget forces ItemsAllocated to stop keeping
		/// track of those items, allowing more items to be created, if necessary.
		/// You must call this if you choose not to Release an item back to the pool.
		/// </summary>
		/// <param name="numItems">Number of items to stop tracking.</param>
		public void Forget(uint numItems)
		{
			ItemsAllocated = ItemsAllocated - numItems;

			// Pool new items, if necessary.
			for (uint i = ItemsAllocated; i < MinItems; i++)
			{
				T newItem = new T();

				if (OnCreate != null)
				{
					OnCreate(ref newItem);
				}

				items[newItem] = new PoolItem<T>(newItem);
				ItemsAllocated++;
			}
		}

		#endregion
	}

	public class PoolItem<T> : IComparable<PoolItem<T>>
	{
		public T item;
		public DateTime timeCreated;

		#region IComparable Members

		public int CompareTo(PoolItem<T> obj)
		{
			return timeCreated.CompareTo(obj.timeCreated);
		}

		#endregion

		#region Public Constructors

		public PoolItem(T item)
		{
			this.item = item;
			timeCreated = DateTime.Now;
		}

		#endregion
	}

    internal struct PoolAcquisition<T> : IDisposable
        where T : new()
    {
        public T Item;

        private ObjectPool<T> Pool;

        public PoolAcquisition(T item, ObjectPool<T> pool)
        {
#if DEBUG
            leakDetector = new Sentinel();
#endif
            Item = item;
            Pool = pool;
        }

        public void Dispose()
        {
            Pool.Release(Item);

            // It's a bad error if someone forgets to call Dispose,
            // so in Debug builds, we put a finalizer in to detect
            // the error. If Dispose is called, we suppress the
            // finalizer.
#if DEBUG
            GC.SuppressFinalize(leakDetector);
#endif
        }

#if DEBUG
        // (In Debug mode, we make it a class so that we can add a finalizer
        // in order to detect when the object is not freed.)
        private class Sentinel
        {
            ~Sentinel()
            {
                // If this finalizer runs, someone somewhere failed to
                // call Dispose, which means we've failed to leave
                // a monitor!
                System.Diagnostics.Debug.Fail("Undisposed pool acquisition");
            }
        }
        private Sentinel leakDetector;
#endif
    }
}
