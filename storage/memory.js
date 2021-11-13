'use strict'

const LRUCache = require('mnemonist/lru-cache')
const nullLogger = require('abstract-logging')
const StorageInterface = require('./interface')
const { findMatchingIndexes, findNotMatching, bsearchIndex } = require('../util')

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
    this.log = options.log || nullLogger

    this.init()
  }

  init () {
    this.store = new LRUCache(this.size)
    this.keysReferences = new Map()
    this.referencesKeys = new Map()
  }

  /**
   * retrieve the value by key
   * @param {string} key
   * @returns {undefined|*} undefined if key not found or expired
   */
  async get (key) {
    this.log.debug({ msg: 'acd/storage/memory.get', key })

    const entry = this.store.get(key)
    if (entry) {
      this.log.debug({ msg: 'acd/storage/memory.get, entry', entry, now: now() })
      if (entry.start + entry.ttl > now()) {
        this.log.debug({ msg: 'acd/storage/memory.get, key is NOT expired', key, entry })
        return entry.value
      }
      this.log.debug({ msg: 'acd/storage/memory.get, key is EXPIRED', key, entry })

      // no need to wait for key to be removed

      setImmediate(() => this.remove(key))
    }
  }

  /**
   * set value by key
   * @param {string} key
   * @param {*} value
   * @param {?number} [ttl=0] - ttl in seconds; zero means key will not be stored
   * @param {?string[]} references
   */
  async set (key, value, ttl, references) {
    this.log.debug({ msg: 'acd/storage/memory.set', key, value, ttl, references })

    ttl = Number(ttl)
    if (!ttl || ttl < 0) {
      return
    }
    const existingKey = this.store.has(key)
    const removed = this.store.setpop(key, { value, ttl, start: now() })
    this.log.debug({ msg: 'acd/storage/memory.set, evicted', removed })
    if (removed && removed.evicted) {
      this.log.debug({ msg: 'acd/storage/memory.set, remove evicted key', key: removed.key })
      this._removeReferences([removed.key])
    }

    if (!references) {
      return
    }

    // references must be unique
    references = [...new Set(references)]

    // clear old references
    let referencesToAdd, oldReferences
    if (existingKey) {
      oldReferences = this.keysReferences.get(key)
      this.log.debug({ msg: 'acd/storage/memory.set, current keys-references', key, references: oldReferences })
      if (oldReferences) {
        oldReferences.sort()
        references.sort()
        const referencesToRemove = findNotMatching(references, oldReferences)

        // remove key in old references
        for (const reference of referencesToRemove) {
          const keys = this.referencesKeys.get(reference)
          if (!keys) { continue }
          const index = bsearchIndex(keys, key)
          if (index < 0) { continue }
          keys.splice(index, 1)

          if (keys.length < 1) {
            this.referencesKeys.delete(reference)
            continue
          }
          this.referencesKeys.set(reference, keys)
        }
      }
    }

    if (!referencesToAdd) {
      // TODO we can probably get referencesToAdd and referencesToRemove in a single loop
      referencesToAdd = oldReferences ? findNotMatching(oldReferences, references) : references
    }
    for (let i = 0; i < referencesToAdd.length; i++) {
      const reference = referencesToAdd[i]
      let keys = this.referencesKeys.get(reference)
      if (keys) {
        this.log.debug({ msg: 'acd/storage/memory.set, add reference-key', key, reference })
        keys.push(key)
        keys.sort()
      } else {
        keys = [key]
      }
      this.log.debug({ msg: 'acd/storage/memory.set, set reference-keys', keys, reference })
      this.referencesKeys.set(reference, keys)
    }

    this.keysReferences.set(key, references)
  }

  /**
   * remove an entry by key
   * @param {string} key
   * @returns {boolean} indicates if key was removed
   */
  async remove (key) {
    this.log.debug({ msg: 'acd/storage/memory.remove', key })

    const removed = this._removeKey(key)
    this._removeReferences([key])
    return removed
  }

  /**
   * @param {string} key
   * @returns {boolean}
   */
  _removeKey (key) {
    this.log.debug({ msg: 'acd/storage/memory._removeKey', key })
    if (!this.store.has(key)) {
      return false
    }
    this.store.set(key, undefined)
    return true
  }

  /**
   * @param {string[]} keys
   */
  _removeReferences (keys) {
    this.log.debug({ msg: 'acd/storage/memory._removeReferences', keys })

    const referencesToRemove = new Set()
    for (let i = 0; i < keys.length; i++) {
      const key = keys[i]

      const references = this.keysReferences.get(key)
      if (!references) {
        continue
      }

      for (let j = 0; j < references.length; j++) {
        referencesToRemove.add(references[j])
      }

      this.log.debug({ msg: 'acd/storage/memory._removeReferences, delete key-references', key })
      this.keysReferences.delete(key)
    }

    this._removeReferencesKeys([...referencesToRemove], keys)
  }

  /**
   * @param {!string[]} references
   * @param {string[]} keys
   */
  _removeReferencesKeys (references, keys) {
    keys.sort()
    this.log.debug({ msg: 'acd/storage/memory._removeReferencesKeys', references, keys })
    for (let i = 0; i < references.length; i++) {
      const reference = references[i]
      // working on the original stored array
      const referencesKeys = this.referencesKeys.get(reference)
      this.log.debug({ msg: 'acd/storage/memory._removeReferencesKeys, get reference-key', reference, keys, referencesKeys })
      if (!referencesKeys) continue

      const referencesToRemove = findMatchingIndexes(keys, referencesKeys)
      // cannot happen that referencesToRemove is empty
      // because this function is triggered only by _removeReferences
      // and "keys" are from tis.keyReferences
      // if (referencesToRemove.length < 1) { continue }

      this.log.debug({ msg: 'acd/storage/memory._removeReferencesKeys, removing', reference, referencesToRemove, referencesKeys })

      if (referencesToRemove.length === referencesKeys.length) {
        this.log.debug({ msg: 'acd/storage/memory._removeReferencesKeys, delete', reference })
        this.referencesKeys.delete(reference)
        continue
      }

      for (let j = referencesToRemove.length - 1; j >= 0; j--) {
        this.log.debug({ msg: 'acd/storage/memory._removeReferencesKeys, remove', reference, referencesKeys, at: referencesToRemove[j] })
        referencesKeys.splice(referencesToRemove[j], 1)
      }
    }
  }

  /**
   * @param {string[]} references
   * @returns {string[]} removed keys
   */
  async invalidate (references) {
    this.log.debug({ msg: 'acd/storage/memory.invalidate', references })

    const removed = []
    for (let i = 0; i < references.length; i++) {
      const reference = references[i]
      const keys = this.referencesKeys.get(reference)
      this.log.debug({ msg: 'acd/storage/memory.invalidate, remove keys on reference', reference, keys })
      if (!keys) {
        continue
      }

      for (let j = 0; j < keys.length; j++) {
        const key = keys[j]
        this.log.debug({ msg: 'acd/storage/memory.invalidate, remove key on reference', reference, key })
        if (this._removeKey(key)) {
          removed.push(key)
        }
      }

      this.log.debug({ msg: 'acd/storage/memory.invalidate, remove references of', reference, keys })
      this._removeReferences([...keys])
    }

    return removed
  }

  /**
   * remove all entries if name is not provided
   * remove entries where key starts with name if provided
   * @param {?string} name
   * @return {string[]} removed keys
   */
  async clear (name) {
    this.log.debug({ msg: 'acd/storage/memory.clear', name })

    if (!name) {
      this.store.clear()
      this.referencesKeys.clear()
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

    const removed = []
    // remove all keys at first, then references
    for (let i = 0; i < keys.length; i++) {
      if (this._removeKey(keys[i])) {
        removed.push(keys[i])
      }
    }

    this._removeReferences(removed)

    return removed
  }

  async refresh () {
    this.log.debug({ msg: 'acd/storage/memory.refresh' })

    this.init()
  }
}

let _timer

function now () {
  if (_timer !== undefined) {
    return _timer
  }
  _timer = Math.floor(Date.now() / 1000)
  setTimeout(_clearTimer, 1000).unref()
  return _timer
}

function _clearTimer () {
  _timer = undefined
}

module.exports = StorageMemory
