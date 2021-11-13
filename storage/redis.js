'use strict'

const stringify = require('safe-stable-stringify')
const nullLogger = require('abstract-logging')
const StorageInterface = require('./interface')

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
    super(options)
    this.store = options.client
    this.log = options.log || nullLogger
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
    } catch (err) {
      this.log.error({ msg: 'acd/storage/redis.set error', err, key, ttl, references })
    }
    // TODO clear references of expired keys
  }

  /**
   * remove all entries if name is not provided
   * remove entries where key starts with name if provided
   * TODO sync references
   * @param {?string} name
   */
  async remove (key) {
    this.log.debug({ msg: 'acd/storage/redis.remove', key })
    try {
      this.store.del(key)
    } catch (err) {
      this.log.error({ msg: 'acd/storage/redis.remove error', err, key })
    }
  }

  /**
   * TODO sync references
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

      const keys = await this.store.keys(name + '*')
      this.log.debug({ msg: 'acd/storage/redis.clear keys', keys })

      const removes = keys.map(key => ['del', key])
      await this.store.pipeline(removes).exec()
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
