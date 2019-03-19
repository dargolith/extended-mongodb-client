import MongoClient from 'mongodb';
import * as R from 'ramda';
// import uuidv1 from 'uuid/v1';

const idToRid = R.map(obj =>
  R.when(
    R.has('_id'),
    R.compose(
      R.omit(['_id']),
      R.evolve({ rid: rid => rid.toString() }),
      R.assoc('rid', obj._id),
    ),
  )(obj),
);

export default class MongoDb {
  constructor(connectionString) {
    this.connectionString = connectionString;
  }

  async usingDb(func) {
    let dbClient;
    try {
      dbClient = await MongoClient.connect(this.connectionString, { useNewUrlParser: true });
      const db = await dbClient.db();
      return await func(db);
    } finally {
      if (dbClient) dbClient.close();
    }
  }

  dropDatabase(dbName) {
    return this.usingDb(db => db.dropDatabase(), dbName);
  }

  insertOne(collection, record) {
    return this.usingDb(async db => {
      const rec = await db.collection(collection).insertOne(record);
      return idToRid(rec.ops)[0];
    });
  }

  insertMany(collection, records, options) {
    return this.usingDb(async db => {
      const rec = await db.collection(collection).insertMany(records, options);
      return idToRid(rec.ops);
    });
  }

  find(collection, filter = {}, skip = 0, limit = 0, projection = {}, sort) {
    // Fix projections, support for both array of properties and mongodb projection object
    if (Array.isArray(projection))
      projection = R.o(R.fromPairs, R.map(key => [key, 1]))(projection);
    if (!R.isEmpty(projection) && !R.has('rid')(projection)) projection.rid = 0;
    if (R.has('rid')(projection)) {
      projection._id = projection.rid;
      projection = R.omit(['rid'])(projection);
    }

    return this.usingDb(async db => {
      const records = await db
        .collection(collection)
        .find(filter)
        .project(projection)
        .sort(sort)
        .skip(skip)
        .limit(limit)
        .toArray();
      return idToRid(records);
    });
  }

  findOne(collection, filter = {}, projection) {
    return this.usingDb(async db => {
      const record = await db.collection(collection).findOne(filter, projection);
      return idToRid([record])[0];
    });
  }

  updateMany(collection, update, filter = {}, options) {
    return this.usingDb(async db => {
      const reply = await db.collection(collection).updateMany(filter, update, options);
      return reply.modifiedCount || 0;
    });
  }

  deleteMany(collection, filter, options) {
    return this.usingDb(async db => {
      const reply = await db.collection(collection).deleteMany(filter, options);
      return reply.deletedCount || 0;
    });
  }

  createIndex(collection, keys, options) {
    return this.usingDb(db => db.collection(collection).createIndex(keys, options));
  }

  // async renameCollection(fromCollection, toCollection, options) {
  //   return this.usingDb(db => db.renameCollection(fromCollection, toCollection, options));
  // }
  //
  // convertToCapped(collection, limit, size, sort) {
  //   return this.usingDb(async db => {
  //     // Create new collection
  //     const tempCollection = `${collection}_temp_${uuidv1()}`;
  //     await db.createCollection(tempCollection, { capped: true, size, max: limit });
  //
  //     // Copy data to new collection
  //     const records = await db
  //       .collection(tempCollection)
  //       .find()
  //       .sort(sort)
  //       .limit(limit)
  //       .toArray();
  //     await db.collection(tempCollection).insertMany(records);
  //
  //     // Delete old collection
  //     await db.dropCollection(collection);
  //
  //     // Rename new collection
  //     db.renameCollection(tempCollection, collection);
  //   });
  // }
  //
  // convertFromCapped(collection) {
  //   return this.usingDb(async db => {
  //     // Create new collection
  //     const tempCollection = `${collection}_temp_${uuidv1()}`;
  //     await db.createCollection(tempCollection);
  //
  //     // Copy data to new collection
  //     const records = await db
  //       .collection(tempCollection)
  //       .find()
  //       .toArray();
  //     await db.collection(tempCollection).insertMany(records);
  //
  //     // Delete old collection
  //     await db.dropCollection(collection);
  //
  //     // Rename new collection
  //     db.renameCollection(tempCollection, collection);
  //   });
  // }
}
