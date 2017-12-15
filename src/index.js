import dbClient from 'mongodb';

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

  async insertOne(record, collection) {
    const db = await dbClient.connect(`${this.connectionString}/${this.dbName}`);
    const rec = await db.collection(collection).insertOne(record);
    db.close();
    return this._idToRid(rec.ops)[0];
  }

  async find(collection, filter = {}, skip = 0, limit = 0, projection, sort) {
    const db = await dbClient.connect(`${this.connectionString}/${this.dbName}`);
    const records = await db
      .collection(collection)
      .find(filter, projection)
      .sort(sort)
      .skip(skip)
      .limit(limit)
      .toArray();
    db.close();
    return this._idToRid(records);
  }

  async deleteMany(collection, filter, options) {
    const db = await dbClient.connect(`${this.connectionString}/${this.dbName}`);
    const result = await db.collection(collection).deleteMany(filter, options);
    db.close();
    return result.deletedCount || 0;
  }
}
