import test from 'blue-tape';
import MongoDb from '../src/index';

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
