'use strict'

const { kValues, kStorage, kTTL, kSize, kOnDedupe, kOnHit, kOnMiss } = require('./symbol')
const stringify = require('safe-stable-stringify')

class Cache {
  /**
   * TODO signature
   * @param {Options} opts
   * @param {Storage} opts.storage
   * @param {number?} [opts.ttl=0] - in seconds; default zero, it means no cache, only dedupe
   * @param {function} opts.onDedupe
   * @param {function} opts.onHit
   * @param {function} opts.onMiss
   */
  constructor (opts) {
    // TODO validate storage
    // TODO validate options
    opts = opts || {}
    this[kValues] = {}
    this[kStorage] = opts.storage
    this[kTTL] = opts.ttl || 0
    // TODO? this[kSize] = opts.size || 1024
    this[kOnDedupe] = opts.onDedupe || noop
    this[kOnHit] = opts.onDedupe || noop
    this[kOnMiss] = opts.onDedupe || noop
  }

  // TODO signature
  define (key, opts, func) {
    if (typeof opts === 'function') {
      func = opts
      opts = {}
    }

    if (key && this[key]) {
      throw new Error(`${key} is already defined in the cache or it is a forbidden name`)
    }

    opts = opts || {}

    if (typeof func !== 'function') {
      throw new TypeError(`Missing the function parameter for '${key}'`)
    }

    const serialize = opts.serialize
    if (serialize && typeof serialize !== 'function') {
      throw new TypeError('serialize must be a function')
    }

    const references = opts.references
    if (references && typeof references !== 'function') {
      throw new TypeError('references must be a function')
    }

    // TODO we could even have a different storage for each key
    const storage = opts.storage || this[kStorage]
    const ttl = opts.ttl || this[kTTL]
    // const size = opts.size || this[kSize]
    const onDedupe = opts.onDedupe || this[kOnDedupe]
    const onHit = opts.onHit || this[kOnHit]
    const onMiss = opts.onMiss || this[kOnMiss]

    const wrapper = new Wrapper(func, key, serialize, references, /*size, */storage, ttl, onDedupe, onHit, onMiss)

    this[kValues][key] = wrapper
    this[key] = wrapper.add.bind(wrapper)
  }

  async clear (key, value) {
    if (key) {
      await this[kValues][key].clear(value)
      return
    }

    for (const wrapper of Object.values(this[kValues])) {
      wrapper.clear()
    }
  }
}

class Wrapper {
  // TODO signature
  constructor (func, key, serialize, references, /*size, */storage, ttl, onDedupe, onHit, onMiss) {
    // TODO do we want to limit dedupe size?
    this.dedupes = new Map()
    this.func = func
    this.key = key
    this.serialize = serialize
    this.references = references

    this.storage = storage
    this.ttl = ttl
    this.onDedupe = onDedupe // TODO bind data
    this.onHit = onHit // TODO bind data
    this.onMiss = onMiss // TODO bind data
  }

  async wrapFunction (args, key) {
    return async () => {
      const data = await this.storage.get(key)
      if (data) {
        this.onHit()
        return data
      }

      this.onMiss()

      // TODO check this block for leaks and issues
      // it should be safe because it's wrapped by query.promise
      const result = await this.func(args, key)

      if (this.ttl < 1) {
        return result
      }

      if (!this.references) {
        await this.storage.set(key, result, this.ttl)
        return result
      }

      const references = this.references(args, key, result)
      await this.storage.set(key, result, this.ttl, references)

      return result
    }
  }

  buildPromise (query, args, key) {
    // wrap the original func to sync storage
    // TODO move to a function
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
        this.storage.remove(key)
      })
  }

  getKey (args) {
    const id = this.serialize ? this.serialize(args) : args
    return typeof id === 'string' ? id : stringify(id)
  }

  add (args) {
    const key = this.getKey(args)

    let query = this.dedupes.get(key)
    if (!query) {
      query = new Query()
      this.buildPromise(query, args, key)
      this.dedupes.set(key, query)
    } else {
      this.onDedupe()
    }

    return query.promise
  }

  async clear (value) {
    if (value) {
      const key = this.getKey(value)
      this.dedupes.set(key, undefined)
      await this.storage.remove(key)
      return
    }
    await this.storage.clear()
    this.dedupes.clear()
  }
}

class Query {
  constructor () {
    this.promise = null
  }
}

function noop () { }

module.exports.Cache = Cache
