'use strict'

const t = require('tap')
const createStorage = require('../storage')
const { promisify } = require('util')
const Redis = require('ioredis')

const sleep = promisify(setTimeout)

// TODO see https://github.com/fastify/fastify-redis/blob/master/.github/workflows/ci.yml
const redisClient = new Redis()

const { test, before, beforeEach, teardown } = t

before(async () => {
  await redisClient.flushall()
})

teardown(async () => {
  await redisClient.quit()
})

test('storage redis', async (t) => {
  test('should get an instance with default options', async (t) => {
    const storage = createStorage('redis', { client: redisClient })

    t.ok(typeof storage.get === 'function')
    t.ok(typeof storage.set === 'function')
    t.ok(typeof storage.remove === 'function')
    t.ok(typeof storage.invalidate === 'function')
    t.ok(typeof storage.refresh === 'function')
  })

  test('get', async (t) => {
    beforeEach(async () => {
      await redisClient.flushall()
    })

    test('should get a value by a key previously stored', async (t) => {
      const storage = createStorage('redis', { client: redisClient })

      await storage.set('foo', 'bar', 100)

      t.equal(await storage.get('foo'), 'bar')
    })

    test('should get undefined retrieving a non stored key', async (t) => {
      const storage = createStorage('redis', { client: redisClient })

      await storage.set('foo', 'bar', 100)

      t.equal(await storage.get('no-foo'), undefined)
    })

    test('should get undefined retrieving an expired value', async (t) => {
      const storage = createStorage('redis', { client: redisClient })

      await storage.set('foo', 'bar', 10)
      await sleep(100)

      t.equal(await storage.get('foo'), undefined)
    })

    test('should not thow on error', async (t) => {
      t.plan(3)
      const storage = createStorage('redis', {
        client: null,
        log: {
          debug: () => {},
          error: (error) => {
            t.equal(error.msg, 'acd/storage/redis.get error')
            t.equal(error.key, 'foo')
          }
        }
      })

      t.equal(await storage.get('foo'), undefined)
    })
  })

  test('set', async (t) => {
    beforeEach(async () => {
      await redisClient.flushall()
    })

    test('should set a value, with ttl', async (t) => {
      const storage = createStorage('redis', { client: redisClient })
      await storage.set('foo', 'bar', 100)

      const value = await storage.store.get('foo')
      t.equal(JSON.parse(value), 'bar')

      const ttl = await storage.store.pttl('foo')
      t.ok(ttl > 90)
      t.ok(ttl < 110)
    })

    test('should not set a value with ttl < 1', async (t) => {
      const storage = createStorage('redis', { client: redisClient })

      await storage.set('foo', 'bar', 0)

      t.equal(await storage.get('foo'), undefined)
    })

    test('should set a value with references', async (t) => {
      const storage = createStorage('redis', { client: redisClient })
      await storage.set('foo', 'bar', 100, ['fooers'])

      const value = await storage.store.get('foo')
      t.equal(JSON.parse(value), 'bar')

      const references = await storage.store.smembers('fooers')
      t.same(references, ['foo'])
    })

    test('should not set a references twice', async (t) => {
      const storage = createStorage('redis', { client: redisClient })
      await storage.set('foo', 'bar', 100, ['fooers'])
      await storage.set('foo', 'new-bar', 100, ['fooers'])

      const value = await storage.store.get('foo')
      t.equal(JSON.parse(value), 'new-bar')

      const references = await storage.store.smembers('fooers')
      t.same(references, ['foo'])
    })

    test('should add a key to an existing reference list', async (t) => {
      const storage = createStorage('redis', { client: redisClient })

      await storage.set('foo1', 'bar1', 100, ['fooers'])
      await storage.set('foo2', 'bar2', 100, ['fooers'])

      const references = await storage.store.smembers('fooers')
      t.same(references, ['foo1', 'foo2'])
    })

    test('should not thow on error', async (t) => {
      t.plan(3)
      const storage = createStorage('redis', {
        client: null,
        log: {
          debug: () => {},
          error: (error) => {
            t.equal(error.msg, 'acd/storage/redis.set error')
            t.equal(error.key, 'foo')
          }
        }
      })

      t.doesNotThrow(() => storage.set('foo', 'bar', 1))
    })
  })

  test('remove', async (t) => {
    test('should remove an existing key', async (t) => {
      const storage = createStorage('redis', { client: redisClient })
      await storage.set('foo', 'bar', 10e3, ['fooers'])

      await storage.remove('foo')

      t.equal(await storage.get('foo'), undefined)
    })

    test('should remove an non existing key', async (t) => {
      const storage = createStorage('redis', { client: redisClient })
      await storage.set('foo', 'bar', 10e3, ['fooers'])

      await storage.remove('fooz')

      t.equal(await storage.get('foo'), 'bar')
      t.equal(await storage.get('fooz'), undefined)
    })

    test('should not thow on error', async (t) => {
      t.plan(3)
      const storage = createStorage('redis', {
        client: null,
        log: {
          debug: () => {},
          error: (error) => {
            t.equal(error.msg, 'acd/storage/redis.remove error')
            t.equal(error.key, 'foo')
          }
        }
      })

      t.doesNotThrow(() => storage.remove('foo'))
    })
  })

  test('invalidate', async (t) => {
    test('should remove storage keys by references', async (t) => {
      const storage = createStorage('redis', { client: redisClient })
      await storage.set('foo~1', 'bar', 1e3, ['fooers', 'foo:1'])
      await storage.set('foo~2', 'baz', 1e3, ['fooers', 'foo:2'])
      await storage.set('boo~1', 'fiz', 1e3, ['booers', 'boo:1'])

      await storage.invalidate(['fooers'])

      t.equal(await storage.get('foo~1'), undefined)
      t.equal(await storage.get('foo~2'), undefined)
      t.equal(await storage.get('boo~1'), 'fiz')
    })

    test('should not remove storage keys by not existing reference', async (t) => {
      const storage = createStorage('redis', { client: redisClient })
      await storage.set('foo~1', 'bar', 1e3, ['fooers', 'foo:1'])
      await storage.set('foo~2', 'baz', 1e3, ['fooers', 'foo:2'])
      await storage.set('boo~1', 'fiz', 1e3, ['booers', 'boo:1'])

      await storage.invalidate(['buzzers'])

      t.equal(await storage.get('foo~1'), 'bar')
      t.equal(await storage.get('foo~2'), 'baz')
      t.equal(await storage.get('boo~1'), 'fiz')
    })

    test('should not thow on error', async (t) => {
      t.plan(3)
      const storage = createStorage('redis', {
        client: null,
        log: {
          debug: () => {},
          error: (error) => {
            t.equal(error.msg, 'acd/storage/redis.invalidate error')
            t.same(error.references, ['pizzers'])
          }
        }
      })

      t.doesNotThrow(() => storage.invalidate(['pizzers']))
    })
  })

  test('clear', async (t) => {
    test('should clear the whole storage', async (t) => {
      const storage = createStorage('redis', { client: redisClient })
      await storage.set('foo', 'bar', 10e3, ['fooers'])
      await storage.set('baz', 'buz', 10e3, ['bazers'])

      await storage.clear()

      t.equal(await storage.store.dbsize(), 0)
    })

    test('should clear only keys with common name', async (t) => {
      const storage = createStorage('redis', { client: redisClient })
      await storage.set('foo~1', 'bar', 10e3, ['fooers'])
      await storage.set('foo~2', 'baz', 10e3, ['bazers'])
      await storage.set('boo~1', 'fiz', 10e3, ['booers'])

      await storage.clear('foo~')

      t.equal(await storage.get('foo~1'), undefined)
      t.equal(await storage.get('foo~2'), undefined)
      t.equal(await storage.get('boo~1'), 'fiz')
    })

    test('should not thow on error', async (t) => {
      t.plan(3)
      const storage = createStorage('redis', {
        client: null,
        log: {
          debug: () => {},
          error: (error) => {
            t.equal(error.msg, 'acd/storage/redis.clear error')
            t.equal(error.name, 'foo')
          }
        }
      })

      t.doesNotThrow(() => storage.clear('foo'))
    })
  })

  test('refresh', async (t) => {
    test('should start a new storage', async (t) => {
      const storage = createStorage('redis', { client: redisClient })
      await storage.set('foo', 'bar', 10e3, ['fooers'])

      await storage.refresh()

      t.equal(await storage.store.dbsize(), 0)
    })

    test('should not thow on error', async (t) => {
      t.plan(2)
      const storage = createStorage('redis', {
        client: null,
        log: {
          debug: () => {},
          error: (error) => {
            t.equal(error.msg, 'acd/storage/redis.refresh error')
          }
        }
      })

      t.doesNotThrow(() => storage.refresh())
    })
  })
})
