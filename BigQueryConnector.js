//Test Schema
const row_data_schema = {
    configuration: {
      load: {
        sourceFormat: 'NEWLINE_DELIMITED_JSON',
        destinationTable: {
          projectId: PropertiesService.getScriptProperties().getProperties().project_id,
          datasetId: PropertiesService.getScriptProperties().getProperties().dataset_id,
          tableId: 'test'
        },
        schema: {
          fields: [
            {name: 'test_1', type: 'STRING'},
            {name: 'test_2', type: 'STRING'},
            {name: 'test_3', type: 'STRING'}
          ]
        }
      }
    }
  };

//Main script

//Run a custom query
function runQuery(query_string) {
  var request = {
    query: query_string.toString()
  };
  var queryResults = BigQuery.Jobs.query(request, PropertiesService.getScriptProperties().getProperties().project_id);
  var jobId = queryResults.jobReference.jobId;
  console.log('Executing query:\n' + query_string + '\nWith job ID: ' + jobId);
  // Check on status of the Query Job.
  var sleepTimeMs = 500;
  while (!queryResults.jobComplete) {
    Utilities.sleep(sleepTimeMs);
    sleepTimeMs *= 2;
    queryResults = BigQuery.Jobs.getQueryResults(projectId, jobId);
  }
  if (queryResults.errors != null) {
    console.error('Errors: ' + queryResults.errors);
  }
  return queryResults;
}

//Create a table with name and custom schema
function createTable(table_name,table_schema) {
  var table_ref = {
    tableReference: {
      projectId: PropertiesService.getScriptProperties().getProperties().project_id,
      datasetId: PropertiesService.getScriptProperties().getProperties().dataset_id,
      tableId: table_name
    },
  };
  var schema = Object.assign({},table_ref,table_schema);
  var result = BigQuery.Tables.insert(schema,PropertiesService.getScriptProperties().getProperties().project_id,PropertiesService.getScriptProperties().getProperties().dataset_id);
  console.log(result);
}

//Create a insert job
function insertRow(data,data_schema) {
  var blob = Utilities.newBlob(data);
  var result = BigQuery.Jobs.insert(data_schema,PropertiesService.getScriptProperties().getProperties().project_id,blob.setContentType('application/json'));
  var job_id = result.jobReference.jobId;
    console.log('Load data to BigQuery start with job ID: ' + job_id);
  while (result.status.state == 'PENDING' || result.status.state == 'RUNNING' ) {
    var sleepTimeMs = 500;
    Utilities.sleep(sleepTimeMs);
    sleepTimeMs *= 2;
    result = BigQuery.Jobs.get(PropertiesService.getScriptProperties().getProperties().project_id,job_id);
  }
  console.log('Job status: '+ result.status.state);
  if (typeof result.status.errors != 'undefined') {
    console.error('Error: ' + result.status.errors);
  }
}

//Remove table function
function deleteTable(table_id) {
  if (tableExists(table_id) == true){
    console.log('Removing ' + table_id);
    var result = BigQuery.Tables.remove(PropertiesService.getScriptProperties().getProperties().project_id,PropertiesService.getScriptProperties().getProperties().dataset_id,table_id);
    if (tableExists(table_id) == false) {
      return result = 'Table has been removed!';
    }
    return result = 'Table has not been removed!';
  }
  var result = 'Table not found!';
  return result;
}

function tableExists(table_id) {
  try {
    // Initialize client that will be used to send requests. This client only needs to be created
    // once, and can be reused for multiple requests.
    table = BigQuery.Tables.get(PropertiesService.getScriptProperties().getProperties().project_id,PropertiesService.getScriptProperties().getProperties().dataset_id,table_id);
    if (table.exist = true) {
      console.log('Table already exist');
      return true;
    } else {
      console.log("Table not found");
      return false;
    }
  } catch (e) {
    console.log("Table not found. \n" + e.toString());
    return false;
  }
}

//Initial properties for this script
function loadBigQueryConnectorProperties() {
  PropertiesService.getScriptProperties().setProperties({project_id: "development", dataset_id: "test"}, false);
}

//Test function
function conector_test() {
  console.log(PropertiesService.getScriptProperties().getProperties())

  //Create Table Sample
  /*var schema = {
    schema: {
      fields: [
        {name: 'test_1', type: 'STRING'},
        {name: 'test_2', type: 'STRING'},
        {name: 'test_3', type: 'STRING'}
      ]
    }
  };
  createTable('test',schema);*/

  //Load job sample
  /*var data = [
    {
      'test_1': '1',
      'test_2': '2',
      'test_3': '3',
    },
    {
      'test_1': '4',
      'test_2': '5',
      'test_3': '6'
    },
    {
      'test_1': '7',
      'test_2': '8',
      'test_3': '9'
    },
    {
      'test_1': '10',
      'test_2': '11',
      'test_3': '12'
    }
  ];
  console.log(data.map(JSON.stringify).join('\n'));
  insertRow(data.map(JSON.stringify).join('\n'),row_data_schema);*/
}
