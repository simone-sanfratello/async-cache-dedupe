'use strict'

const path = require('path')
const stringify = require('safe-stable-stringify')
const nullLogger = require('abstract-logging')
const Piscina = require('piscina')
const { AbortController } = require('abort-controller')
const StorageInterface = require('../interface')

/**
 * @typedef StorageRedisOptions
 * @property {!RedisClient} client
 * @property {?Logger} log
 */

class StorageRedis extends StorageInterface {
  /**
   * @param {StorageRedisOptions} options
   */
  constructor (options) {
    // TODO validate options
    // TODO options to enable/disable invalidation
    super(options)
    this.log = options.log || nullLogger

    // TODO init function
    // if (!options.client) {
    //   throw new Error('Redis client is required')
    // }
    this.store = options.client

    // TODO validation, init function
    // TODO start a worker thread for pub/sub and move sync references work there

    if (options && options.client && options.client.options) {
      // TODO test, try/catch
      console.log('*** storage worker')
      this.references = { abortController: new AbortController() }
      this.references.process = new Piscina({ 
        idleTimeout: 60e3,
        filename: path.resolve(__dirname, 'sync-references.js') 
      })
      this.references.process.run(stringify(options.client.options), { 
        signal: this.references.signal 
      })
        .then((response) => {
          console.log('*** storage worker response', response)
        })
        .catch((err) => {
          console.error('*** storage worker error', err)
        })
      // TODO? .catch(noop)
    }
  }

  end () {
    if (!this.references) {
      return
    }
    try {
      this.references.abortController.abort()
      console.log('***** storage abort')
    } catch (err) {
      console.log('***** storage end error', err)
      // TODO cancelled
    }
  }

  /**
   * @param {string} key
   * @returns {undefined|*} undefined if key not found
   */
  async get (key) {
    this.log.debug({ msg: 'acd/storage/redis.get', key })

    try {
      const value = await this.store.get(key)
      if (!value) {
        return undefined
      }
      return JSON.parse(value)
    } catch (err) {
      this.log.error({ msg: 'acd/storage/redis.get error', err, key })
    }
  }

  /**
   * @param {string} key
   * @param {*} value
   * @param {number} ttl - ttl in seconds; zero means key will not be stored
   * @param {?string[]} references
   */
  async set (key, value, ttl, references) {
    // TODO validate key, cant contains * or so
    this.log.debug({ msg: 'acd/storage/redis.set key', key, value, ttl, references })

    ttl = Number(ttl)
    if (!ttl || ttl < 0) {
      return
    }

    try {
      await this.store.set(key, stringify(value), 'EX', ttl)

      if (!references) {
        return
      }

      const writes = []
      for (let i = 0; i < references.length; i++) {
        const reference = references[i]
        this.log.debug({ msg: 'acd/storage/redis.set reference', key, reference })
        writes.push(['sadd', 'r:' + reference, key])
      }
      await this.store.pipeline(writes).exec()
      // TODO write key->references
    } catch (err) {
      this.log.error({ msg: 'acd/storage/redis.set error', err, key, ttl, references })
    }
  }

  /**
   * remove an entry by key
   * @param {string} key
   * @returns {boolean} indicates if key was removed
   */
  async remove (key) {
    this.log.debug({ msg: 'acd/storage/redis.remove', key })
    try {
      const removed = this._removeKey(key)
      this._removeReferences([key])
      return removed
    } catch (err) {
      this.log.error({ msg: 'acd/storage/redis.remove error', err, key })
      return false
    }
  }

  async _removeKey (key) {
    this.log.debug({ msg: 'acd/storage/redis.remove', key })
    try {
      return await this.store.del(key) > 0
    } catch (err) {
      this.log.error({ msg: 'acd/storage/redis.remove error', err, key })
    }
  }

  /**
   * @param {string[]} keys
   */
  _removeReferences (keys) {
    this.log.debug({ msg: 'acd/storage/redis._removeReferences', keys })

    // TODO remove references->keys and keys->references
  }

  /**
   * @param {string[]} references
   */
  async invalidate (references) {
    this.log.debug({ msg: 'acd/storage/redis.invalidate', references })

    try {
      const reads = references.map(reference => ['smembers', 'r:' + reference])
      const keys = await this.store.pipeline(reads).exec()

      this.log.debug({ msg: 'acd/storage/redis.invalidate keys', keys })

      const writes = []
      for (let i = 0; i < keys.length; i++) {
        const key0 = keys[i][1]
        this.log.debug({ msg: 'acd/storage/redis.invalidate got keys to be invalidated', keys: key0 })
        for (let j = 0; j < key0.length; j++) {
          const key1 = key0[j]
          this.log.debug({ msg: 'acd/storage/redis.del key' + key1 })
          writes.push(['del', key1])
        }
      }

      await this.store.pipeline(writes).exec()

      // TODO remove references->keys and keys->references
    } catch (err) {
      this.log.error({ msg: 'acd/storage/redis.invalidate error', err, references })
    }
  }

  /**
   * @param {string} name
   */
  async clear (name) {
    this.log.debug({ msg: 'acd/storage/redis.clear', name })

    try {
      if (!name) {
        await this.store.flushall()
        return
      }

      const keys = await this.store.keys(`${name}*`)
      this.log.debug({ msg: 'acd/storage/redis.clear keys', keys })

      const removes = keys.map(key => ['del', key])
      await this.store.pipeline(removes).exec()

      // TODO remove references->keys and keys->references
    } catch (err) {
      this.log.error({ msg: 'acd/storage/redis.clear error', err, name })
    }
  }

  async refresh () {
    try {
      await this.store.flushall()
    } catch (err) {
      this.log.error({ msg: 'acd/storage/redis.refresh error', err })
    }
  }
}

module.exports = StorageRedis
