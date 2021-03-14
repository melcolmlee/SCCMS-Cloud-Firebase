// The Cloud Functions for Firebase SDK to create Cloud Functions and setup triggers.
const functions = require("firebase-functions"),
  PubSub = require(`@google-cloud/pubsub`),
  admin = require("firebase-admin"),
  BigQuery = require('@google-cloud/bigquery');
 
const app = admin.initializeApp();
const firestore = app.firestore();  
const bigqueryClient = new BigQuery();


exports.updateFireStore = functions.pubsub.topic('doorlockdata').onPublish((message) => {

    // Get attribute of the PubSub message JSON body.
    let devID, md, rd, temp, hum, ldr, ts = null;
    try {
      devID = message.json.devID;
      md = message.json.md;
      rd = message.json.rd;
      temp = message.json.temp;
      hum = message.json.hum;
      ldr = message.json.ldr;
      ts = message.json.ts;
      console.log(`Update from  ${devID} at ${ts}`);
    } catch (e) {
    console.error('PubSub message was not JSON', e);
    }
    
    //Update firestore
    let id = null;
    const FieldValue = admin.firestore.FieldValue;
    const collection = firestore.collection('truck_info');
    const updateDoc = collection.where ("devID", "==", devID).limit(1).get().then(query => {
        id = query.docs[0].id;
        collection.doc(id).update({'temp' : temp});
        return console.log("Firestore update Success");
      });

    //Update bigquery
    //TODO
    // Inserts the JSON objects into my_dataset:my_table.

    /**
     * TODO(developer): Uncomment the following lines before running the sample.
     */
    // const datasetId = 'my_dataset';
    // const tableId = 'my_table';
    const rows = [
        {name: 'Tom', age: 30},
        {name: 'Jane', age: 32},
      ];
  
      // Insert data into a table
      await bigquery
        .dataset(datasetId)
        .table(tableId)
        .insert(rows);
      console.log(`Inserted ${rows.length} rows`);
    // [END bigquery_table_insert_rows]

    });