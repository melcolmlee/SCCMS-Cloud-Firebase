// The Cloud Functions for Firebase SDK to create Cloud Functions and setup triggers.
const functions = require("firebase-functions"),
  PubSub = require('@google-cloud/pubsub'),
  admin = require("firebase-admin"),
  {BigQuery} = require('@google-cloud/bigquery');

var unirest = require('unirest'); 
 
const app = admin.initializeApp();
const firestore = app.firestore();  
const bigqueryClient = new BigQuery();


exports.updateReadingsFirestore = functions.pubsub.topic('truckupdate').onPublish((message) => {

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
        collection.doc(id).update({'temp' : temp, 'md' : md, 'rd' : rd, 'hum' : hum, 'ldr' : ldr, 'ts' : ts});
        return console.log("Firestore update Success");
      });

    });

exports.updateReadingsBigQuery = functions.pubsub.topic('truckupdate').onPublish((message) => {

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
      
    //Get current weather temp
    //var req = unirest.get('https://api.data.gov.sg/v1/environment/air-temperature').end(function (res) { 
    //    if (res.error) throw new Error(res.error);
    //    console.log(res.raw_body);
    //  });
      
    //Update bigquery
    const datasetId = 'truck_readings';
    const tableId = 'readings';
    const rows = [
        {devID: devID, temp: temp, hum: hum, md: md, rd: rd, ts: ts}
      ];
    
    // Insert data into a table
      try{
      const updatebigquery= bigqueryClient.dataset(datasetId).table(tableId).insert(rows);
      console.log(`Inserted ${rows}`);
      }catch (e) {
        console.error('Error inserting rows into bigquery', e);
      }
    });