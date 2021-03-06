'use strict';

const test = require('ava');
const intoStream = require('into-stream');
const getStream = require('get-stream');

const update = require('../');

test('throws when missing keyField', t => {
    const err = t.throws(() => update());
    t.is(err.message, 'must provide keyField');
});

test('throws when changes is not a Map', t => {
    const err = t.throws(() => update({ keyField: 'test', versionField: '_v', changes: {} }));
    t.is(err.message, 'changes must be a Map');
});

test('passes through records with no change', async t => {
    const existing = { _v: 1, test: 'test', foo: 'bar' };

    const changes = new Map();
    const stream = update({ keyField: 'test', versionField: '_v', changes });

    const start = intoStream.object([existing]);

    const expected = [existing];
    const actual = await getStream.array(start.pipe(stream));

    t.deepEqual(expected, actual);
});

test('appends new changes', async t => {
    const existing = { _v: 1, test: 'test', foo: 'bar' };
    const added = { _v: 2, test: 'testing', foo: 'baz' };

    const changes = new Map();
    changes.set('testing', added);

    const stream = update({ keyField: 'test', versionField: '_v', changes });

    const start = intoStream.object([existing]);

    const actual = await getStream.array(start.pipe(stream));
    const expected = [existing, added];

    t.deepEqual(actual, expected);
});

test('appends new changes (batches)', async t => {
    const existing = { _v: 1, test: 'test', foo: 'bar' };
    const added = { _v: 2, test: 'testing', foo: 'baz' };

    const changes = new Map();
    changes.set('testing', added);

    const stream = update({ keyField: 'test', versionField: '_v', changes, batches: true });

    const start = intoStream.object([existing]);

    const actual = await getStream.array(start.pipe(stream));
    const expected = [[existing], [added]];

    t.deepEqual(actual, expected);
});

test('replaces newer change', async t => {
    const existing = { _v: 1, test: 'test', foo: 'bar' };
    const added = { _v: 2, test: 'test', foo: 'baz' };

    const changes = new Map();
    changes.set('test', added);

    const stream = update({ keyField: 'test', versionField: '_v', changes });

    const start = intoStream.object([existing]);

    const actual = await getStream.array(start.pipe(stream));
    const expected = [added];

    t.deepEqual(actual, expected);
});

test('skips newer change', async t => {
    const existing = { _v: 2, test: 'test', foo: 'bar' };
    const outOfDate = { _v: 1, test: 'test', foo: 'baz' };

    const changes = new Map();
    changes.set('test', outOfDate);

    const stream = update({ keyField: 'test', versionField: '_v', changes });

    const start = intoStream.object([existing]);

    const actual = await getStream.array(start.pipe(stream));
    const expected = [existing];

    t.deepEqual(actual, expected);
});

test('processes batches but does not emit them when not configured', async t => {
    const existing = { _v: 1, test: 'test', foo: 'bar' };
    const newer = { _v: 2, test: 'test', foo: 'baz' };

    const changes = new Map();
    changes.set('test', newer);

    const stream = update({ keyField: 'test', versionField: '_v', changes });

    const start = intoStream.object([[existing]]);

    const actual = await getStream.array(start.pipe(stream));
    const expected = [newer];

    t.deepEqual(actual, expected);
});

test('processes batches and emits them when configured', async t => {
    const existing = { _v: 1, test: 'test', foo: 'bar' };
    const added = { _v: 2, test: 'test', foo: 'baz' };

    const changes = new Map();
    changes.set('test', added);

    const stream = update({ keyField: 'test', versionField: '_v', changes, batches: true });

    const start = intoStream.object([[existing]]);

    const actual = await getStream.array(start.pipe(stream));
    const expected = [[added]];

    t.deepEqual(actual, expected);
});
