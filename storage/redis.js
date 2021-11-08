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

  async set (key, value, ttl, references) {
    this.log.debug({ msg: 'acd/storage/redis.set key', key, value, ttl, references })

    ttl = Number(ttl)
    if (!ttl || ttl < 0) {
      return
    }

    try {
      await this.store.set(key, stringify(value), 'PX', ttl)

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

  async remove (key) {
    this.log.debug({ msg: 'acd/storage/redis.remove', key })

    this.store.del(key)
    // TODO remove key in references? do it lazy/gc?
  }

  async invalidate (references) {
    this.log.debug({ msg: 'acd/storage/redis.invalidate', references })
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
        // TODO! if not store key => this._store.sdel(reference, key)
        await this.store.del(keys[j])
      }
    }
    // TODO update references removing deleted keys?
  }

  async clear (name) {
    await this.store.flushall()
    // TODO remove keys starts with name
  }

  async refresh () {
    await this.store.flushall()
  }
}

module.exports = StorageRedis
