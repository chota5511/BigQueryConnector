//Calculate total pages
function getTotalPages(total_records,per_page) {
  var total_pages = Math.floor(total_records / per_page);

  if (total_records % per_page > 0) {
    total_pages++;
  }
  return total_pages;
}

function jsonArrayToArray(json_array_object) {
  var tmp = [];
  for(var i in json_array_object) {
    tmp.push(json_array_object[i]);
    console.log(tmp[i]);
  }
  return tmp;
}

function combineArray(array_1,array_2) {
  for (var i in array_2) {
    array_1.push(array_2[i]);
  }
  return array_1;
}

function getKey(csv) {
  var key = [];

  //Get key from csv
  for(var i in csv[0]) {
    var tmp = csv[0][i];
    if(isNaN(tmp[0]) == false){
      tmp = "_" + tmp;
    }
    key.push(tmp.split(' ').join('_').split('(').join('').split(')').join('').split('-').join("_").toLowerCase());
  }
  return key;
}

function generateSchema(project_id,dataset_id,table_id,key) {
  var schema = {
    configuration: {
      load: {
        sourceFormat: 'NEWLINE_DELIMITED_JSON',
        destinationTable: {
          projectId: project_id,
          datasetId: dataset_id,
          tableId: table_id
        },
        schema: {
          fields: []
        }
      }
    }
  };

  var fields = [];
  for(var i in key) {
    var tmp = {};
    tmp["name"] = key[i];
    tmp["type"] = "STRING";
    fields.push(tmp);
  }
  //fields = fields.map(JSON.stringify).join('\n');
  console.log(fields);
  schema["configuration"]["load"]["schema"]["fields"] = fields
  return schema;
}

function csv2array(csv){
  var key = [];
  var result = [];

  //Get key from csv
  for(var i in csv[0]) {
    var tmp = csv[0][i];
    if(isNaN(tmp[0]) == false){
      tmp = "_" + tmp;
    }
    key.push(tmp.split(' ').join('_').split('(').join('').split(')').join('').split('-').join("_").toLowerCase());
  }

  for(var i = 1; i < csv.length; i++){
    var tmp = {}
    for(var j in csv[i]) {
      tmp[key[j]] = csv[i][j]
    }
    result.push(tmp);
  }
  return result;
}

function dateToYYYYMMDD(date,delimiter){
  var str = date.getFullYear().toString() + delimiter + date.getMonth().toString() + delimiter + date.getDate().toString()
  return str;
}
