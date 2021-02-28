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
    let Status, DoorID, NFCValue, timecollected = null;
    try {
      Status = message.json.Status;
      DoorID = message.json.DoorID;
      NFCValue = message.json.NFCValue;
      timecollected = message.json.timecollected;
      console.log(`Door status is ${Status} and Door ID is ${DoorID}`);
    } catch (e) {
    console.error('PubSub message was not JSON', e);
    }
    
    //Update firestore
    let id = null;
    const FieldValue = admin.firestore.FieldValue;
    const collection = firestore.collection('door_access');
    const updateDoc = collection.where ("DoorID", "==", DoorID).limit(1).get().then(query => {
        id = query.docs[0].id;
        collection.doc(id).update({'Status' : Status, 'NFCValue' : NFCValue, TimeStamp: FieldValue.serverTimestamp()});
        const addDoc = firestore.collection('door_access_log').add({'DoorID' : DoorID, 'access_time': FieldValue.serverTimestamp(), 'status' : Status, 'user_uuid' : id});
        return console.log("Update Success");
      });
    console.log ("Created doc successfully");

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