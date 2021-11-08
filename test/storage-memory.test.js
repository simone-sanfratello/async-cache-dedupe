'use strict'

const t = require('tap')
const createStorage = require('../storage')
const { promisify } = require('util')

const sleep = promisify(setTimeout)

const { test } = t

test('storage memory', async (t) => {
  test('should get an instance with default options', async (t) => {
    const storage = createStorage('memory')

    t.ok(typeof storage.get === 'function')
    t.ok(typeof storage.set === 'function')
    t.ok(typeof storage.remove === 'function')
    t.ok(typeof storage.invalidate === 'function')
    t.ok(typeof storage.refresh === 'function')

    t.equal(storage.store.capacity, 1024)
  })

  test('get', async (t) => {
    test('should get a value by a key previously stored', async (t) => {
      const storage = createStorage('memory')

      await storage.set('foo', 'bar', 100)

      t.equal(await storage.get('foo'), 'bar')
    })

    test('should get undefined retrieving a non stored key', async (t) => {
      const storage = createStorage('memory')

      await storage.set('foo', 'bar', 100)

      t.equal(await storage.get('no-foo'), undefined)
    })

    test('should get undefined retrieving an expired value', async (t) => {
      const storage = createStorage('memory')

      await storage.set('foo', 'bar', 10)
      await sleep(50)

      t.equal(await storage.get('foo'), undefined)
    })
  })

  test('set', async (t) => {
    test('should set a value, with ttl', async (t) => {
      const storage = createStorage('memory')
      await storage.set('foo', 'bar', 100)

      const stored = storage.store.get('foo')

      t.equal(stored.value, 'bar')
      t.ok(stored.expires > Date.now())
      await sleep(100)
      t.ok(stored.expires < Date.now() + 100)
    })

    test('should not set a value with ttl < 1', async (t) => {
      const storage = createStorage('memory')

      await storage.set('foo', 'bar', 0)

      t.equal(await storage.get('foo'), undefined)
    })

    test('should set a value with references', async (t) => {
      const storage = createStorage('memory')
      await storage.set('foo', 'bar', 100, ['fooers'])

      const stored = storage.store.get('foo')
      t.equal(stored.value, 'bar')

      const reference = storage.references.get('fooers')
      t.same(reference, ['foo'])
    })

    test('should not set a references twice', async (t) => {
      const storage = createStorage('memory')
      await storage.set('foo', 'bar', 100, ['fooers'])
      await storage.set('foo', 'new-bar', 100, ['fooers'])

      const stored = storage.store.get('foo')
      t.equal(stored.value, 'new-bar')

      const reference = storage.references.get('fooers')
      t.same(reference, ['foo'])
    })

    test('should add a key to an existing reference list', async (t) => {
      const storage = createStorage('memory')

      await storage.set('foo1', 'bar1', 100, ['fooers'])
      await storage.set('foo2', 'bar2', 100, ['fooers'])

      const reference = storage.references.get('fooers')
      t.same(reference, ['foo1', 'foo2'])
    })
  })

  test('remove', async (t) => {
    test('should remove an existing key', async (t) => {
      const storage = createStorage('memory')
      await storage.set('foo', 'bar', 10e3, ['fooers'])

      await storage.remove('foo')

      t.equal(await storage.get('foo'), undefined)
    })

    test('should remove an non existing key', async (t) => {
      const storage = createStorage('memory')
      await storage.set('foo', 'bar', 10e3, ['fooers'])

      await storage.remove('fooz')

      t.equal(await storage.get('foo'), 'bar')
      t.equal(await storage.get('fooz'), undefined)
    })
  })

  test('invalidate', async (t) => {
    test('should remove storage keys by references', async (t) => {
      const storage = createStorage('memory')
      await storage.set('foo~1', 'bar', 1e3, ['fooers', 'foo:1'])
      await storage.set('foo~2', 'baz', 1e3, ['fooers', 'foo:2'])
      await storage.set('boo~1', 'fiz', 1e3, ['booers', 'boo:1'])

      await storage.invalidate(['fooers'])

      t.equal(await storage.get('foo~1'), undefined)
      t.equal(await storage.get('foo~2'), undefined)
      t.equal(await storage.get('boo~1'), 'fiz')
    })

    test('should not remove storage keys by not existing reference', async (t) => {
      const storage = createStorage('memory')
      await storage.set('foo~1', 'bar', 1e3, ['fooers', 'foo:1'])
      await storage.set('foo~2', 'baz', 1e3, ['fooers', 'foo:2'])
      await storage.set('boo~1', 'fiz', 1e3, ['booers', 'boo:1'])

      await storage.invalidate(['buzzers'])

      t.equal(await storage.get('foo~1'), 'bar')
      t.equal(await storage.get('foo~2'), 'baz')
      t.equal(await storage.get('boo~1'), 'fiz')
    })
  })

  test('clear', async (t) => {
    test('should clear the whole storage', async (t) => {
      const storage = createStorage('memory')
      await storage.set('foo', 'bar', 10e3, ['fooers'])
      await storage.set('baz', 'buz', 10e3, ['bazers'])

      await storage.clear()

      t.equal(storage.store.size, 0)
      t.equal(storage.references.size, 0)
    })

    test('should clear only keys with common name', async (t) => {
      const storage = createStorage('memory')
      await storage.set('foo~1', 'bar', 10e3, ['fooers'])
      await storage.set('foo~2', 'baz', 10e3, ['bazers'])
      await storage.set('boo~1', 'fiz', 10e3, ['booers'])

      await storage.clear('foo~')

      t.equal(await storage.get('foo~1'), undefined)
      t.equal(await storage.get('foo~2'), undefined)
      t.equal(await storage.get('boo~1'), 'fiz')
    })
  })

  test('refresh', async (t) => {
    test('should start a new storage', async (t) => {
      const storage = createStorage('memory')
      await storage.set('foo', 'bar', 10e3, ['fooers'])

      await storage.refresh()
      t.equal(storage.store.size, 0)
      t.equal(storage.references.size, 0)
    })
  })
})
