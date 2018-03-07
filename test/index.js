import test from 'blue-tape';
import MongoDb from '../src/index';

// Note: The following tests requires a mongodb instance to run on the default port (27017)

let db;

test('Create instance', async t => {
  db = new MongoDb('mongodb://localhost:27017', 'testdb');
  t.ok(db instanceof MongoDb);
});

test('insertOne', async t => {
  const reply = await db.insertOne('testCollection', {
    testKey: 'testValue',
    testKey2: 'testValue2',
  });
  t.equals(reply.testKey, 'testValue');
});

test('find', async t => {
  const reply = await db.find('testCollection', { testKey: 'testValue', testKey2: 'testValue2' });
  t.equals(reply[0].testKey, 'testValue');
});

test('updateMany', async t => {
  const reply = await db.updateMany(
    'testCollection',
    {
      $set: {
        testKey2: 'testValue2New',
        newKey: 'newKeyValue',
      },
    },
    { testKey: 'testValue' },
  );
  t.equals(reply, 1);
});

test('deleteMany', async t => {
  const reply = await db.deleteMany('testCollection', { testKey: 'testValue' });
  t.equals(reply, 1);
});

test('createIndex', async t => {
  const collection = 'testCollection';
  const keys = { property1: -1, property2: 1 };
  const options = { sparse: true };
  const reply = await db.createIndex(collection, keys, options);
  t.equals(typeof reply, 'string');
});
