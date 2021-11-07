'use strict'

const LRUCache = require('mnemonist/lru-cache')
const StorageInterface = require('./interface')

const DEFAULT_CACHE_SIZE = 1024

/**
 * @typedef StorageMemoryOptions
 * @property {?number} [size=1024]
 * @property {Logger} log
 */

class StorageMemory extends StorageInterface {
  /**
   * in-memory storage
   * @param {StorageMemoryOptions} options
   */
  constructor (options = {}) {
    // TODO validate options
    // logger is mandatory
    super(options)
    this.store = new LRUCache(options.size || DEFAULT_CACHE_SIZE)
    this.references = new Map()
    this.log = options.log
  }

  async get (key) {
    const entry = this.store.get(key)
    if (entry) {
      if (entry.expires > Date.now()) {
        return entry.value
      }
      this.store.set(key, undefined)
    }
  }

  async remove (key) {
    console.log('storage.remove', key)
    this.store.set(key, undefined)
  }

  async set (key, value, ttl, references) {
    this.store.set(key, { value, expires: Date.now() + ttl })

    if (!references) {
      return
    }
    for (let i = 0; i < references.length; i++) {
      const reference = references[i]
      let keys = this.references.get(reference)
      if (keys) {
        if (keys.includes(key)) {
          continue
        }
        keys.push(key)
      }
      keys = [key]
      this.references.set(reference, keys)
    }
  }

  async invalidate (references) {
    for (let i = 0; i < references.length; i++) {
      const reference = references[i]
      const keys = this.references.get(reference)
      if (!keys) {
        continue
      }
      for (let j = 0; j < keys.length; j++) {
        this.store.set(keys[j], undefined)
      }
    }
    // TODO update references removing deleted keys?
  }

  async clear (name) {
    if (!name) {
      this.store.clear()
      this.references.clear()
      return
    }
    this.store.forEach((value, key) => {
      if (key.indexOf(name) === 0) {
        this.store.set(key, undefined)
      }
    })
  }

  async refresh () {
    this.store = new LRUCache(this.options.size)
    this.references = new Map()
  }
}

module.exports = StorageMemory
