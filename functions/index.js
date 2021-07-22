// The Cloud Functions for Firebase SDK to create Cloud Functions and setup triggers.
const functions = require("firebase-functions"),
  PubSub = require('@google-cloud/pubsub'),
  admin = require("firebase-admin"),
  {BigQuery} = require('@google-cloud/bigquery');

const fetch = require('node-fetch');
 
const app = admin.initializeApp();
const firestore = app.firestore();  
const bigqueryClient = new BigQuery();

//Get current weather temp
let weather_temp = null;   
const getWeatherTemp = fetch('https://api.data.gov.sg/v1/environment/air-temperature').then(res => res.json()).then(data => weather_temp = data.items[0].readings[1].value).then(() => console.log (weather_temp));

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
   
    //Update bigquery
    const datasetId = 'truck_readings';
    const tableId = 'readings';
    const rows = [
        {devID: devID, temp: temp, hum: hum, md: md, rd: rd, ts: ts, weather_temp:weather_temp}
      ];
    
    // Insert data into a table
      try{
      const updatebigquery= bigqueryClient.dataset(datasetId).table(tableId).insert(rows);
      console.log(`Inserted ${rows}`);
      console.log("Weather temp :", weather_temp)
      }catch (e) {
        console.error('Error inserting rows into bigquery', e);
      }
    });