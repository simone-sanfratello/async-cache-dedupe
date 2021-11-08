'use strict'

const { kValues, kStorage, kTTL, kOnDedupe, kOnHit, kOnMiss } = require('./symbol')
const stringify = require('safe-stable-stringify')
const createStorage = require('./storage')

class Cache {
  /**
   * TODO signature
   * @param {Options} opts
   * @param {number?} [opts.ttl=0] - in seconds; default 0 seconds, means no cache, only do dedupe
   * @param {Storage?} [opts.storage] - the storage to use; default is in-memory storage
   * @param {function} opts.onDedupe
   * @param {function} opts.onHit
   * @param {function} opts.onMiss
   */
  constructor (opts) {
    // TODO validate storage
    // TODO validate options
    opts = opts || {}
    this[kValues] = {}
    this[kTTL] = opts.ttl || 0
    this[kStorage] = opts.storage || createStorage()
    this[kOnDedupe] = opts.onDedupe || noop
    this[kOnHit] = opts.onHit || noop
    this[kOnMiss] = opts.onMiss || noop
  }

  // TODO signature
  define (name, opts, func) {
    if (typeof opts === 'function') {
      func = opts
      opts = {}
    }

    if (name && this[name]) {
      throw new Error(`${name} is already defined in the cache or it is a forbidden name`)
    }

    opts = opts || {}

    if (typeof func !== 'function') {
      throw new TypeError(`Missing the function parameter for '${name}'`)
    }

    const serialize = opts.serialize
    if (serialize && typeof serialize !== 'function') {
      throw new TypeError('serialize must be a function')
    }

    const references = opts.references
    if (references && typeof references !== 'function') {
      throw new TypeError('references must be a function')
    }

    // TODO doc we could even have a different storage for each key
    const storage = opts.storage || this[kStorage]
    const ttl = opts.ttl || this[kTTL]
    const onDedupe = opts.onDedupe || this[kOnDedupe]
    const onHit = opts.onHit || this[kOnHit]
    const onMiss = opts.onMiss || this[kOnMiss]

    const wrapper = new Wrapper(func, name, serialize, references, storage, ttl, onDedupe, onHit, onMiss)

    this[kValues][name] = wrapper
    this[name] = Cache.add.bind(wrapper)
  }

  async clear (name, value) {
    if (name) {
      await this[kValues][name].clear(value)
      return
    }

    const clears = []
    for (const wrapper of Object.values(this[kValues])) {
      clears.push(wrapper.clear())
    }
    await Promise.all(clears)
  }

  async get (name, key) {
    // TODO validate
    console.log('Cache.get')
    return this[kValues][name].get(key)
  }

  async set (name, key, value, ttl, references) {
    // TODO validate
    console.log('Cache.set')
    return this[kValues][name].set(key, value, ttl, references)
  }

  async invalidate (name, references) {
    // TODO validate
    console.log('Cache.invalidate')
    return this[kValues][name].invalidate(references)
  }
}

class Wrapper {
  // TODO signature
  constructor (func, name, serialize, references, storage, ttl, onDedupe, onHit, onMiss) {
    // TODO do we want to limit dedupe size?
    this.dedupes = new Map()
    this.func = func
    this.name = name
    this.serialize = serialize
    this.references = references

    this.storage = storage
    this.ttl = ttl
    this.onDedupe = onDedupe
    this.onHit = onHit
    this.onMiss = onMiss
  }

  getKey (args) {
    const id = this.serialize ? this.serialize(args) : args
    return typeof id === 'string' ? id : stringify(id)
  }

  getStorageKey (key) {
    return `${this.name}~${key}`
  }

  getStorageName () {
    return `${this.name}~`
  }

  add (args) {
    const key = this.getKey(args)

    let query = this.dedupes.get(key)
    if (!query) {
      query = new Query()
      this.buildPromise(query, args, key)
      this.dedupes.set(key, query)
    } else {
      this.onDedupe(key)
    }

    return query.promise
  }

  /**
   * wrap the original func to sync storage
   */
  async wrapFunction (args, key) {
    const storageKey = this.getStorageKey(key)
    const data = await this.storage.get(storageKey)
    if (data !== undefined) {
      this.onHit(key)
      return data
    }

    this.onMiss(key)

    const result = await this.func(args, key)

    if (this.ttl < 1) {
      return result
    }

    if (!this.references) {
      await this.storage.set(storageKey, result, this.ttl)
      return result
    }

    const references = await this.references(args, key, result)
    // TODO validate references?
    await this.storage.set(storageKey, result, this.ttl, references)

    return result
  }

  buildPromise (query, args, key) {
    query.promise = this.wrapFunction(args, key)

    // we fork the promise chain on purpose
    query.promise
      .then(result => {
        // clear the dedupe once done
        this.dedupes.set(key, undefined)
        return result
      })
      // TODO do we want an onError event?
      .catch(() => {
        this.dedupes.set(key, undefined)
        // TODO option to remove key from storage on error?
        // we may want to relay on cache if the original function got error
        // then we probably need more option for that
        this.storage.remove(this.getStorageKey(key)).catch(noop)
      })
  }

  async clear (value) {
    // TODO validate
    if (value) {
      const key = this.getKey(value)
      this.dedupes.set(key, undefined)
      await this.storage.remove(this.getStorageKey(key))
      return
    }
    await this.storage.clear(this.getStorageName())
    this.dedupes.clear()
  }

  async get (key) {
    console.log('Wrapper.get')
    return this.storage.get(key)
  }

  async set (key, value, ttl, references) {
    console.log('Wrapper.set')
    return this.storage.set(key, value, ttl, references)
  }

  async invalidate (references) {
    console.log('Wrapper.invalidate')
    return this.storage.invalidate(references)
  }
}

class Query {
  constructor () {
    this.promise = null
  }
}

function noop () { }

module.exports.Cache = Cache
