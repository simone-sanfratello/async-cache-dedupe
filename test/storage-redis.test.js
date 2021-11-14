'use strict'

const t = require('tap')
const createStorage = require('../storage')
const { promisify } = require('util')
const Redis = require('ioredis')

const sleep = promisify(setTimeout)

const redisClient = new Redis()

const { test, skip, before, beforeEach, teardown } = t

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

      storage.end()
    })

    test('should get undefined retrieving a non stored key', async (t) => {
      const storage = createStorage('redis', { client: redisClient })

      await storage.set('foo', 'bar', 100)

      t.equal(await storage.get('no-foo'), undefined)

      storage.end()
    })

    test('should get undefined retrieving an expired value', async (t) => {
      const storage = createStorage('redis', { client: redisClient })

      await storage.set('foo', 'bar', 1)
      await sleep(2000)

      t.equal(await storage.get('foo'), undefined)

      storage.end()
    })

    test('should not throw on error', async (t) => {
      t.plan(3)
      const storage = createStorage('redis', {
        client: {},
        log: {
          debug: () => {},
          error: (error) => {
            t.equal(error.msg, 'acd/storage/redis.get error')
            t.equal(error.key, 'foo')
          }
        }
      })

      t.equal(await storage.get('foo'), undefined)

      storage.end()
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

      const ttl = await storage.store.ttl('foo')
      t.equal(ttl, 100)

      storage.end()
    })

    test('should not set a value with ttl < 1', async (t) => {
      const storage = createStorage('redis', { client: redisClient })

      await storage.set('foo', 'bar', 0)

      t.equal(await storage.get('foo'), undefined)

      storage.end()
    })

    test('should set a value with references', async (t) => {
      const storage = createStorage('redis', { client: redisClient })
      await storage.set('foo', 'bar', 100, ['fooers'])

      const value = await storage.store.get('foo')
      t.equal(JSON.parse(value), 'bar')

      const references = await storage.store.smembers('r:fooers')
      t.same(references, ['foo'])

      storage.end()
    })

    test('should not set a references twice', async (t) => {
      const storage = createStorage('redis', { client: redisClient })
      await storage.set('foo', 'bar', 100, ['fooers'])
      await storage.set('foo', 'new-bar', 100, ['fooers'])

      const value = await storage.store.get('foo')
      t.equal(JSON.parse(value), 'new-bar')

      const references = await storage.store.smembers('r:fooers')
      t.same(references, ['foo'])

      storage.end()
    })

    test('should add a key to an existing reference', async (t) => {
      const storage = createStorage('redis', { client: redisClient })

      await storage.set('foo1', 'bar1', 100, ['fooers'])
      await storage.set('foo2', 'bar2', 100, ['fooers'])

      const references = await storage.store.smembers('r:fooers')
      t.equal(references.length, 2)
      t.ok(references.includes('foo1'))
      t.ok(references.includes('foo2'))

      storage.end()
    })

    skip('should update the key references, case #1', async (t) => {
      const storage = createStorage('redis', { client: redisClient })

      await storage.set('foo', 'bar1', 100, ['fooers', 'mooers'])
      await storage.set('foo', 'bar2', 100, ['booers', 'tooers'])

      let references = await storage.store.smembers('r:fooers')
      t.equal(references.length, 0)

      references = await storage.store.smembers('r:mooers')
      t.equal(references.length, 0)

      references = await storage.store.smembers('r:booers')
      t.equal(references.length, 0)

      references = await storage.store.smembers('r:tooers')
      t.equal(references.length, 0)

      storage.end()
    })

    skip('should update the key references, case #2', async (t) => {
      // TODO
      const storage = createStorage('memory')

      await storage.set('foo', 'bar1', 100, ['fooers', 'mooers'])
      await storage.set('foo', 'bar2', 100, ['mooers', 'tooers'])

      t.equal(storage.referencesKeys.get('fooers'), undefined)
      t.same(storage.referencesKeys.get('mooers'), ['foo'])
      t.same(storage.referencesKeys.get('tooers'), ['foo'])

      t.same(storage.keysReferences.get('foo'), ['mooers', 'tooers'])

      storage.end()
    })

    skip('should update the key references, case #3', async (t) => {
      // TODO
      const storage = createStorage('memory')

      await storage.set('foo', 'bar1', 100, ['a', 'b'])
      await storage.set('foo', 'bar2', 100, ['z', 'b', 'd'])

      t.equal(storage.referencesKeys.get('a'), undefined)
      t.same(storage.referencesKeys.get('b'), ['foo'])
      t.same(storage.referencesKeys.get('d'), ['foo'])
      t.same(storage.referencesKeys.get('z'), ['foo'])

      t.same(storage.keysReferences.get('foo'), ['b', 'd', 'z'])

      storage.end()
    })

    test('should not throw on error', async (t) => {
      t.plan(3)
      const storage = createStorage('redis', {
        client: {},
        log: {
          debug: () => {},
          error: (error) => {
            t.equal(error.msg, 'acd/storage/redis.set error')
            t.equal(error.key, 'foo')
          }
        }
      })

      t.doesNotThrow(() => storage.set('foo', 'bar', 1))

      storage.end()
    })
  })

  test('remove', async (t) => {
    beforeEach(async () => {
      await redisClient.flushall()
    })

    test('should remove an existing key', async (t) => {
      const storage = createStorage('redis', { client: redisClient })
      await storage.set('foo', 'bar', 10, ['fooers'])

      await storage.remove('foo')

      t.equal(await storage.get('foo'), undefined)

      storage.end()
    })

    test('should remove an non existing key', async (t) => {
      const storage = createStorage('redis', { client: redisClient })
      await storage.set('foo', 'bar', 10, ['fooers'])

      await storage.remove('fooz')

      t.equal(await storage.get('foo'), 'bar')
      t.equal(await storage.get('fooz'), undefined)

      storage.end()
    })

    skip('should remove a key but not references if still active', async (t) => {
      // TODO
      const storage = createStorage('memory')
      await storage.set('a', 1, 10, ['fooers', 'vowels'])
      await storage.set('b', 1, 10, ['fooers', 'consonantes'])
      await storage.set('c', 1, 10, ['fooers', 'consonantes'])
      await storage.set('d', 1, 10, ['consonantes'])
      await storage.set('e', 1, 10, ['vowels'])

      await storage.remove('a')

      t.equal(await storage.get('a'), undefined)
      t.equal(await storage.get('b'), 1)
      t.equal(await storage.get('c'), 1)
      t.equal(await storage.get('d'), 1)
      t.equal(await storage.get('e'), 1)

      t.same(storage.referencesKeys.get('fooers'), ['b', 'c'])
      t.same(storage.referencesKeys.get('consonantes'), ['b', 'c', 'd'])
      t.same(storage.referencesKeys.get('vowels'), ['e'])

      t.same(storage.keysReferences.get('a'), undefined)
      t.same(storage.keysReferences.get('b'), ['fooers', 'consonantes'])
      t.same(storage.keysReferences.get('c'), ['fooers', 'consonantes'])
      t.same(storage.keysReferences.get('d'), ['consonantes'])
      t.same(storage.keysReferences.get('e'), ['vowels'])

      storage.end()
    })

    test('should not throw on error', async (t) => {
      t.plan(3)
      const storage = createStorage('redis', {
        client: {},
        log: {
          debug: () => {},
          error: (error) => {
            t.equal(error.msg, 'acd/storage/redis.remove error')
            t.equal(error.key, 'foo')
          }
        }
      })

      t.doesNotThrow(() => storage.remove('foo'))

      storage.end()
    })
  })

  test('invalidate', async (t) => {
    beforeEach(async () => {
      await redisClient.flushall()
    })

    test('should remove storage keys by references', async (t) => {
      const storage = createStorage('redis', { client: redisClient })
      await storage.set('foo~1', 'bar', 1, ['fooers', 'foo:1'])
      await storage.set('foo~2', 'baz', 1, ['fooers', 'foo:2'])
      await storage.set('boo~1', 'fiz', 1, ['booers', 'boo:1'])

      await storage.invalidate(['fooers'])

      t.equal(await storage.get('foo~1'), undefined)
      t.equal(await storage.get('foo~2'), undefined)
      t.equal(await storage.get('boo~1'), 'fiz')

      storage.end()
    })

    test('should not remove storage keys by not existing reference', async (t) => {
      const storage = createStorage('redis', { client: redisClient })
      await storage.set('foo~1', 'bar', 1, ['fooers', 'foo:1'])
      await storage.set('foo~2', 'baz', 1, ['fooers', 'foo:2'])
      await storage.set('boo~1', 'fiz', 1, ['booers', 'boo:1'])

      await storage.invalidate(['buzzers'])

      t.equal(await storage.get('foo~1'), 'bar')
      t.equal(await storage.get('foo~2'), 'baz')
      t.equal(await storage.get('boo~1'), 'fiz')

      storage.end()
    })

    test('should invalide more than one reference at once', async (t) => {
      const storage = createStorage('redis', { client: redisClient })
      await storage.set('foo~1', 'bar', 1, ['fooers', 'foo:1'])
      await storage.set('foo~2', 'baz', 1, ['fooers', 'foo:2'])
      await storage.set('boo~1', 'fiz', 1, ['booers', 'boo:1'])

      await storage.invalidate(['fooers', 'booers'])

      t.equal(await storage.get('foo~1'), undefined)
      t.equal(await storage.get('foo~2'), undefined)
      t.equal(await storage.get('boo~1'), undefined)

      storage.end()
    })

    skip('should remove storage keys by references, but not the ones still alive', async (t) => {
      // TODO
      const storage = createStorage('memory')
      await storage.set('foo~1', 'bar', 1, ['fooers', 'foo:1'])
      await storage.set('foo~boo', 'baz', 1, ['fooers', 'booers'])
      await storage.set('boo~1', 'fiz', 1, ['booers', 'boo:1'])

      const removed = await storage.invalidate(['fooers'])

      t.same(removed, ['foo~1', 'foo~boo'])

      t.equal(await storage.get('foo~1'), undefined)
      t.equal(await storage.get('boo~1'), 'fiz')
      t.equal(await storage.get('foo~boo'), undefined)

      t.equal(storage.referencesKeys.get('fooers'), undefined)
      t.equal(storage.referencesKeys.get('foo:1'), undefined)
      t.same(storage.referencesKeys.get('booers'), ['boo~1'])

      t.equal(storage.keysReferences.get('foo~1'), undefined)
      t.equal(storage.keysReferences.get('foo~boo'), undefined)
      t.same(storage.keysReferences.get('boo~1'), ['booers', 'boo:1'])

      storage.end()
    })

    skip('should remove a keys and references and also linked ones', async (t) => {
      // TODO
      const storage = createStorage('memory')
      await storage.set('a', 1, 10, ['fooers', 'vowels', 'empty'])
      await storage.set('b', 1, 10, ['fooers', 'consonantes'])
      await storage.set('c', 1, 10, ['fooers', 'consonantes'])
      await storage.set('d', 1, 10, ['consonantes'])
      await storage.set('e', 1, 10, ['vowels'])

      await storage.invalidate(['fooers'])

      t.equal(await storage.get('a'), undefined)
      t.equal(await storage.get('b'), undefined)
      t.equal(await storage.get('c'), undefined)
      t.equal(await storage.get('d'), 1)
      t.equal(await storage.get('e'), 1)

      t.same(storage.referencesKeys.get('fooers'), undefined)
      t.same(storage.referencesKeys.get('empty'), undefined)
      t.same(storage.referencesKeys.get('consonantes'), ['d'])
      t.same(storage.referencesKeys.get('vowels'), ['e'])

      t.same(storage.keysReferences.get('a'), undefined)
      t.same(storage.keysReferences.get('b'), undefined)
      t.same(storage.keysReferences.get('c'), undefined)
      t.same(storage.keysReferences.get('d'), ['consonantes'])
      t.same(storage.keysReferences.get('e'), ['vowels'])

      storage.end()
    })

    test('should not throw on error', async (t) => {
      t.plan(3)
      const storage = createStorage('redis', {
        client: {},
        log: {
          debug: () => {},
          error: (error) => {
            t.equal(error.msg, 'acd/storage/redis.invalidate error')
            t.same(error.references, ['pizzers'])
          }
        }
      })

      t.doesNotThrow(() => storage.invalidate(['pizzers']))

      storage.end()
    })
  })

  test('clear', async (t) => {
    beforeEach(async () => {
      await redisClient.flushall()
    })

    test('should clear the whole storage', async (t) => {
      const storage = createStorage('redis', { client: redisClient })
      await storage.set('foo', 'bar', 10, ['fooers'])
      await storage.set('baz', 'buz', 10, ['bazers'])

      await storage.clear()

      t.equal(await storage.store.dbsize(), 0)

      storage.end()
    })

    test('should clear only keys with common name', async (t) => {
      const storage = createStorage('redis', { client: redisClient })
      await storage.set('foo~1', 'bar', 10, ['fooers'])
      await storage.set('foo~2', 'baz', 10, ['bazers'])
      await storage.set('boo~1', 'fiz', 10, ['booers'])

      await storage.clear('foo~')

      t.equal(await storage.get('foo~1'), undefined)
      t.equal(await storage.get('foo~2'), undefined)
      t.equal(await storage.get('boo~1'), 'fiz')

      storage.end()
    })

    skip('should clear a keys and their references', async (t) => {
      // TODO
      const storage = createStorage('memory')
      await storage.set('a-a', 1, 10, ['fooers', 'vowels', 'empty'])
      await storage.set('a-b', 1, 10, ['fooers', 'consonantes'])
      await storage.set('a-c', 1, 10, ['fooers', 'consonantes'])
      await storage.set('b-d', 1, 10, ['consonantes'])
      await storage.set('b-e', 1, 10, ['vowels'])

      await storage.clear('a-')

      t.equal(await storage.get('a-a'), undefined)
      t.equal(await storage.get('a-b'), undefined)
      t.equal(await storage.get('a-c'), undefined)
      t.equal(await storage.get('b-d'), 1)
      t.equal(await storage.get('b-e'), 1)

      t.same(storage.referencesKeys.get('fooers'), undefined)
      t.same(storage.referencesKeys.get('empty'), undefined)
      t.same(storage.referencesKeys.get('consonantes'), ['b-d'])
      t.same(storage.referencesKeys.get('vowels'), ['b-e'])

      t.same(storage.keysReferences.get('a-a'), undefined)
      t.same(storage.keysReferences.get('a-b'), undefined)
      t.same(storage.keysReferences.get('a-c'), undefined)
      t.same(storage.keysReferences.get('b-d'), ['consonantes'])
      t.same(storage.keysReferences.get('b-e'), ['vowels'])

      storage.end()
    })

    test('should not throw on error', async (t) => {
      t.plan(3)
      const storage = createStorage('redis', {
        client: {},
        log: {
          debug: () => {},
          error: (error) => {
            t.equal(error.msg, 'acd/storage/redis.clear error')
            t.equal(error.name, 'foo')
          }
        }
      })

      t.doesNotThrow(() => storage.clear('foo'))

      storage.end()
    })
  })

  test('refresh', async (t) => {
    beforeEach(async () => {
      await redisClient.flushall()
    })

    test('should start a new storage', async (t) => {
      const storage = createStorage('redis', { client: redisClient })
      await storage.set('foo', 'bar', 10, ['fooers'])

      await storage.refresh()

      t.equal(await storage.store.dbsize(), 0)

      storage.end()
    })

    test('should not throw on error', async (t) => {
      t.plan(2)
      const storage = createStorage('redis', {
        client: {},
        log: {
          debug: () => {},
          error: (error) => {
            t.equal(error.msg, 'acd/storage/redis.refresh error')
          }
        }
      })

      t.doesNotThrow(() => storage.refresh())

      storage.end()
    })
  })
})
