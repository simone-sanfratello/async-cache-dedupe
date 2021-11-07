'use strict'

const stringify = require('safe-stable-stringify')
const StorageInterface = require('./interface')

class StorageRedis extends StorageInterface {
  constructor ({ instance, log }) {
    // TODO validate options
    super({ instance, log })
    this.store = instance
    // logger is mandatory
    this.log = log
  }

  async get (key) {
    try {
      this.log.debug({ msg: '[mercurius-cache - redis storage] get key', key })
      const value = await this.store.get(key)
      return JSON.parse(value)
    } catch (err) {
      this.log.error({ msg: '[mercurius-cache - redis storage] error on get', err, key })
    }
  }

  async set (key, value, ttl, references) {
    try {
      this.log.debug({ msg: '[mercurius-cache - redis storage] set key', key, value, ttl, references })
      await this.store.set(key, stringify(value), 'EX', ttl)

      if (!references) {
        return
      }
      for (let i = 0; i < references.length; i++) {
        const reference = references[i]
        // TODO can be done in 1 query? pipeline?
        this.log.debug({ msg: '[mercurius-cache - redis storage] set reference', key, reference })
        this.store.sadd(reference, key)
      }
    } catch (err) {
      this.log.error({ msg: '[mercurius-cache - redis storage] error on set', err, key })
    }
  }

  async invalidate (references) {
    this.log.debug({ msg: '[mercurius-cache - redis storage] invalidate', references })
    // TODO can nested loops be avoided?
    for (let i = 0; i < references.length; i++) {
      const reference = references[i]
      // TODO pipeline?
      const keys = await this.store.smembers(reference)
      this.log.debug({ msg: '[mercurius-cache - redis storage] got keys to invalidate', keys })
      if (!keys || keys.length < 1) {
        continue
      }
      for (let j = 0; j < keys.length; j++) {
        // TODO can be done in 1 query? pipeline?
        this.log.debug({ msg: '[mercurius-cache - redis storage] del key' + keys[j] })
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
