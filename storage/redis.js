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
      for (let i = 0; i < references.length; i++) {
        const reference = references[i]
        // TODO can be done in 1 query? pipeline?
        this.log.debug({ msg: 'acd/storage/redis.set reference', key, reference })
        this.store.sadd(reference, key)
      }
    } catch (err) {
      this.log.error({ msg: 'acd/storage/redis.set error', err, key, ttl, references })
    }
  }

  /**
   * @param {string} key
   */
  async remove (key) {
    this.log.debug({ msg: 'acd/storage/redis.remove', key })
    try {
      this.store.del(key)
    // TODO remove key in references? do it lazy/gc?
    } catch (err) {
      this.log.error({ msg: 'acd/storage/redis.remove error', err, key })
    }
  }

  /**
   * @param {string[]} references
   */
  async invalidate (references) {
    this.log.debug({ msg: 'acd/storage/redis.invalidate', references })

    try {
    // TODO can nested loops be avoided?
      for (let i = 0; i < references.length; i++) {
        const reference = references[i]
        // TODO pipeline?
        const keys = await this.store.smembers(reference)
        this.log.debug({ msg: 'acd/storage/redis.invalidate got keys to be invalidated', keys })
        if (!keys || keys.length < 1) {
          continue
        }
        for (let j = 0; j < keys.length; j++) {
        // TODO can be done in 1 query? pipeline?
          this.log.debug({ msg: 'acd/storage/redis.del key' + keys[j] })
          await this.store.del(keys[j])
        }
      }
    // TODO update references removing deleted keys? gc?
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

      // TODO pipeline
      const tasks = []
      for (let i = 0; i < keys.length; i++) {
        tasks.push(this.store.del(keys[i]))
      }
      await Promise.all(tasks)
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
