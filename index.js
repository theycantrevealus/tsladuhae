var fs = require('fs');
var es = require('event-stream');
var totalData = 0;
var DataSet = {};
var DataSetArr = [];
var starttime;
starttime = getCurrentTimestamp();
console.log(`${starttime} => Start`);

const MongoClient = require('mongodb').MongoClient;
console.log(`${getCurrentTimestamp()} => Preparing SL File`);
readFile('3000.txt', 'SL').then(() => { // Read SL

  // Loop File LMS for comparing
  console.log('Comparing LMS File');
  readFile('500.txt', 'LMS').then(() => {
    console.log(`${getCurrentTimestamp()} - Finish comparing`);
    for (var a in DataSet) {
      DataSetArr.push(DataSet[a]);
    }
    DataSet = {};

    MongoClient.connect('mongodb://localhost/tWow', async (err, client) => {
      if (err) return console.error(err);
      console.log(`Insert Compared Result... ${DataSetArr.length} datas`);
      const db = client.db('data-set');
      const coll = db.collection('sl-set');
      let toInsert = [];
      for (let i = 0; i < DataSetArr.length; i++) {
        toInsert.push(DataSetArr[i]);
        const isLastItem = i === DataSetArr.length - 1;
        if (i % 500000 === 0 || isLastItem) {
          // console.log(`Batch - ${toInsert.length} datas`);
          await coll.insertMany(toInsert, function () {
            console.log(`${getCurrentTimestamp()} - End Write Batch`);
          });
          toInsert = [];
        }
      }
    });
  });
});


async function readFile(filename, type) {
  return new Promise((resolve, reject) => {
    fs.createReadStream(filename)
      .pipe(es.split())
      .pipe(es.mapSync((rowData) => {
        var parsedRow = rowData.split(/\r?\n/);
        parsedRow.forEach((cells) => {
          totalData++;
          var cell = cells.split('|');

          if (type === 'SL') {
            if (DataSet[cell[1]] === undefined) {
              DataSet[cell[1]] = {
                report_id: '',
                sn: '',
                msisdn: '',
                lms_trxtime: '',
                lms_denom: '',
                lms_point: '',
                lms_tier: '',
                sl_trxtime: '',
                sl_denom: '',
                sl_point: '',
                sl_tier: '',
                status: ''
              };
            }

            DataSet[cell[1]] = {
              report_id: '',
              sn: cell[1],
              msisdn: cell[2],
              lms_trxtime: DataSet[cell[1]].lms_trxtime,
              lms_denom: DataSet[cell[1]].lms_denom,
              lms_point: DataSet[cell[1]].lms_point,
              lms_tier: DataSet[cell[1]].lms_tier,
              sl_trxtime: cell[0],
              sl_denom: cell[3],
              sl_point: cell[4],
              sl_tier: cell[5],
              status: 'SL Only'
            };
          } else { // LMS
            if (DataSet[cell[1]] === undefined) {
              DataSet[cell[1]] = {
                report_id: '',
                sn: '',
                msisdn: '',
                lms_trxtime: '',
                lms_denom: '',
                lms_point: '',
                lms_tier: '',
                sl_trxtime: '',
                sl_denom: '',
                sl_point: '',
                sl_tier: '',
                status: 'LMS Only'
              };
            } else {
              if (
                cell[0] === DataSet[cell[1]].sl_trxtime &&
                cell[1] === DataSet[cell[1]].sn &&
                cell[2] === DataSet[cell[1]].msisdn &&
                cell[3] === DataSet[cell[1]].sl_denom &&
                cell[4] === DataSet[cell[1]].sl_point &&
                cell[5] === DataSet[cell[1]].sl_tier
              ) {
                DataSet[cell[1]] = {
                  report_id: '',
                  sn: cell[1],
                  msisdn: cell[2],
                  lms_trxtime: cell[0],
                  lms_denom: cell[3],
                  lms_point: cell[4],
                  lms_tier: cell[5],
                  sl_trxtime: DataSet[cell[1]].sl_trxtime,
                  sl_denom: DataSet[cell[1]].sl_denom,
                  sl_point: DataSet[cell[1]].sl_point,
                  sl_tier: DataSet[cell[1]].sl_tier,
                  status: 'Match'
                };
              } else {
                DataSet[cell[1]] = {
                  report_id: '',
                  sn: cell[1],
                  msisdn: cell[2],
                  lms_trxtime: cell[0],
                  lms_denom: cell[3],
                  lms_point: cell[4],
                  lms_tier: cell[5],
                  sl_trxtime: DataSet[cell[1]].sl_trxtime,
                  sl_denom: DataSet[cell[1]].sl_denom,
                  sl_point: DataSet[cell[1]].sl_point,
                  sl_tier: DataSet[cell[1]].sl_tier,
                  status: 'Not Match'
                };
              }
            }
          }
        });
      }).on('error', function (err) {
        console.log(err);
        reject();
      }).on('end', function () {
        resolve();
      }));
  });
}

function getCurrentTimestamp() {
  let today = new Date();
  const yyyy = today.getFullYear();
  let mm = today.getMonth() + 1;
  let dd = today.getDate();
  let dda = '';
  let mma = '';
  let todaya = '';

  if (dd < 10) {
    dda = '0' + dd;
  } else {
    dda = dd + '';
  }

  if (mm < 10) {
    mma = '0' + mm;
  } else {
    mma = mm + '';
  }

  todaya = yyyy + '/' + mma + '/' + dda + ' ' + ((today.getHours() < 10) ? '0' + today.getHours() : today.getHours()) + ':' + ((today.getMinutes() < 10) ? '0' + today.getMinutes() : today.getMinutes()) + ':' + ((today.getSeconds() < 10) ? '0' + today.getSeconds() : today.getSeconds());

  return todaya;
}