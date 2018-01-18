import MongoClient from 'mongodb';

export default class MongoDb {
  constructor(connectionString, dbName) {
    this.connectionString = connectionString;
    this.dbName = dbName;
  }

  _idToRid(arr) {
    return arr.map(r => {
      r.rid = r._id.valueOf();
      delete r._id;
      return r;
    });
  }

  async insertOne(collection, record) {
    let dbClient;
    let rec;
    try {
      dbClient = await MongoClient.connect(this.connectionString);
      const db = await dbClient.db(this.dbName);
      rec = await db.collection(collection).insertOne(record);
    } finally {
      if (dbClient) dbClient.close();
    }
    return this._idToRid(rec.ops)[0];
  }

  async find(collection, filter = {}, skip = 0, limit = 0, projection, sort) {
    let dbClient;
    try {
      dbClient = await MongoClient.connect(this.connectionString);
      const db = await dbClient.db(this.dbName);
      const records = await db
        .collection(collection)
        .find(filter, projection)
        .sort(sort)
        .skip(skip)
        .limit(limit)
        .toArray();
      return this._idToRid(records);
    } finally {
      if (dbClient) dbClient.close();
    }
  }

  async updateMany(collection, update, filter = {}, options) {
    let dbClient;
    let result;
    try {
      dbClient = await MongoClient.connect(this.connectionString);
      const db = await dbClient.db(this.dbName);
      result = await db.collection(collection).updateMany(filter, update, options);
    } finally {
      if (dbClient) dbClient.close();
    }
    return result.modifiedCount;
  }

  async deleteMany(collection, filter, options) {
    let dbClient;
    let result;
    try {
      dbClient = await MongoClient.connect(this.connectionString);
      const db = await dbClient.db(this.dbName);
      result = await db.collection(collection).deleteMany(filter, options);
    } finally {
      if (dbClient) dbClient.close();
    }
    return result.deletedCount || 0;
  }
}
