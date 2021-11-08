'use strict'

const LRUCache = require('mnemonist/lru-cache')
const nullLogger = require('abstract-logging')
const StorageInterface = require('./interface')

const DEFAULT_CACHE_SIZE = 1024

/**
 * @typedef StorageMemoryOptions
 * @property {?number} [size=1024]
 * @property {?Logger} [log]
 */

class StorageMemory extends StorageInterface {
  /**
   * in-memory storage
   * @param {StorageMemoryOptions} options
   */
  constructor (options = {}) {
    // TODO validate options
    super(options)
    this.size = options.size || DEFAULT_CACHE_SIZE
    this.store = new LRUCache(this.size)
    this.references = new Map()
    this.log = options.log || nullLogger
  }

  /**
   * @param {string} key
   * @returns {undefined|*} undefined if key not found
   */
  async get (key) {
    this.log.debug({ msg: 'acd/storage/memory.get', key })

    const entry = this.store.get(key)
    if (entry) {
      if (entry.expires > Date.now()) {
        this.log.debug({ msg: 'acd/storage/memory.get, key is NOT expired', key, entry })
        return entry.value
      }
      this.log.debug({ msg: 'acd/storage/memory.get, key is EXPIRED', key, entry })
      this.store.set(key, undefined)
    }
  }

  async remove (key) {
    this.log.debug({ msg: 'acd/storage/memory.remove', key })

    if (!this.store.has(key)) {
      return
    }
    this.store.set(key, undefined)
    // TODO remove key in references? do it lazy/gc?
  }

  async set (key, value, ttl, references) {
    this.log.debug({ msg: 'acd/storage/memory.set', key, value, ttl, references })

    ttl = Number(ttl)
    if (!ttl || ttl < 0) {
      return
    }
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
      } else {
        keys = [key]
      }
      this.references.set(reference, keys)
    }
  }

  /**
   * @param {string[]} references
   */
  async invalidate (references) {
    this.log.debug({ msg: 'acd/storage/memory.invalidate', references })

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
    // TODO return/callback delete keys?
  }

  async clear (name) {
    this.log.debug({ msg: 'acd/storage/memory.clear', name })

    if (!name) {
      this.store.clear()
      this.references.clear()
      return
    }

    const keys = []
    this.store.forEach((value, key) => {
      this.log.debug({ msg: 'acd/storage/memory.clear, iterate key', key })
      if (key.indexOf(name) === 0) {
        this.log.debug({ msg: 'acd/storage/memory.clear, remove key', key })
        // can't remove here or the loop won't work
        keys.push(key)
      }
    })

    for (let i = 0; i < keys.length; i++) {
      this.store.set(keys[i], undefined)
    }
    // TODO remove key in references? do it lazy/gc?
    // see remove method

    // TODO return/callback removed keys?
  }

  async refresh () {
    this.log.debug({ msg: 'acd/storage/memory.refresh' })

    this.store = new LRUCache(this.size)
    this.references = new Map()
  }
}

module.exports = StorageMemory
