//* Prerequisites:
// - Setup FB App, authorised for FB Insights & Conversion API
// - Get AD_ACCOUNT_ID
// - Get APP_ID from https://developers.facebook.com/apps/<YOUR_APP_ID>/settings/basic/
// - Get ACCESS_TOKEN from https://developers.facebook.com/apps/<YOUR_APP_ID>/settings/basic/
// - Get CLIENT_ID (CLIENT_ID is App ID) from https://developers.facebook.com/apps/<YOUR_APP_ID>/settings/basic/
// - Get CLIENT_SECRET from https://developers.facebook.com/apps/<YOUR_APP_ID>/settings/basic/
// - Get ACCESS_TOKEN from https://developers.facebook.com/apps/<YOUR_APP_ID>/marketing-api/tools/
// - Get BIGQUERY_PROJECT_ID from https://console.cloud.google.com/bigquery?project=<YOUR_BIGQUERY_PROJECT_ID>
// - Get BIGQUERY_DATASET_ID from https://console.cloud.google.com/bigquery?project=<YOUR_BIGQUERY_PROJECT_ID>
// - Get BIGQUERY_TABLE_ID from https://console.cloud.google.com/bigquery?project=<YOUR_BIGQUERY_PROJECT_ID>
const AD_ACCOUNT_ID = 'xxxxxxx'
const CLIENT_ID = `xxxxxxx`;
const CLIENT_SECRET = `xxxxxxx`;
// Place the ACCESS_TOKEN here
const INITIAL_ACCESS_TOKEN = `xxxxxxx`;
const API_URL = `https://graph.facebook.com/v11.0/`;
const BIGQUERY_PROJECT_ID = "development";
const BIGQUERY_DATASET_ID = "raw_data_fb_ads";
const BIGQUERY_TABLE_ID = "fb_ad";
/// END - Prerequisites

//* Do not change
const access_token = PropertiesService.getScriptProperties().getProperties().access_token;
/// END - Do not change


// Post a report request function, get yesterday is default
function requestReport(date_range = "yesterday", level = "ad", fields = "ad_id,ad_name,campaign_id,campaign_name,adset_id,adset_name,impressions,spend,clicks,cpc,objective,actions") {
  var params = `act_${AD_ACCOUNT_ID}/insights?access_token=${access_token}&level=${level}&fields=${fields}&date_preset=${date_range}&time_increment=1&limit=1000`;
  var options = {
    'method' : 'post'
  };
  var url = API_URL + params;
  var result = JSON.parse(UrlFetchApp.fetch(encodeURI(url),options).getContentText());
  console.log(result["report_run_id"]);
  PropertiesService.getScriptProperties().setProperties({"report_run_id": result["report_run_id"]});
}

// Post a report request function by using custom date range
function requestReportByTimeRange(from_date, until_date, level = "ad", fields = "ad_id,ad_name,campaign_id,campaign_name,adset_id,adset_name,impressions,spend,clicks,cpc,objective,actions") {

  var time_range = {
    "since":dateToYYYYMMDD(from_date,"-"),
    "until":dateToYYYYMMDD(until_date,"-")
  };
  console.log(time_range);
  var params = `act_${AD_ACCOUNT_ID}/insights?access_token=${access_token}&level=${level}&fields=${fields}&time_range=${JSON.stringify(time_range)}&time_increment=1&limit=1000`;
  var options = {
    'method' : 'post'
  };
  var url = API_URL + params;
  console.log(url);
  var result = JSON.parse(UrlFetchApp.fetch(encodeURI(url),options).getContentText());
  console.log(result["report_run_id"]);
  PropertiesService.getScriptProperties().setProperties({"report_run_id": result["report_run_id"]});
}

// Check the status of the requested report
function asyncStatusCheck(job_id) {
  var params = `${job_id}?access_token=${access_token}`
  var url = API_URL + params;
  var options = {
    'method' : 'get'
  };

  var result = JSON.parse(UrlFetchApp.fetch(url,options).getContentText());
  console.log(result);
  if(result["async_status"] == "Job Completed") {
    return true;
  }
  return false;
}

//Get report
function getReport() {
  var job_id = PropertiesService.getScriptProperties().getProperty("report_run_id");
  if(job_id != "") {
    if (asyncStatusCheck(job_id) == true) {
      var params = `?report_run_id=${job_id}&format=csv&access_token=${access_token}&locale=en_US`
      var url = `https://www.facebook.com/ads/ads_insights/export_report${params}`;
      var results = Utilities.parseCsv(UrlFetchApp.fetch(url));
      PropertiesService.getScriptProperties().setProperty("report_run_id","");
      return results;
    }
    else {
      console.log("Request does not done yet!");
      return
    }
  }
  else
  {
    console.log("No report to receive!");
    return
  }
}

// Post a all time request report (Cannot run if there are too much data, should segmentize the request)
function requestReportAllTime() {
  requestReport("maximum");
}

// Request default report request (yesterday)
function dailyRequest() {
  requestReport();
}

// Function for load the gotten report
function loadFbAdsToBigQuery() {
  var report = getReport();
  if(report != null) {
    var data = csv2array(report);
    console.log(data);
    var key = getKey(report);
    var schema = generateSchema(BIGQUERY_PROJECT_ID,BIGQUERY_DATASET_ID,BIGQUERY_TABLE_ID,key);
    if(data.length > 0){
      console.log('Load ' + data.length + ' records to BigQuery');
      insertRow(data.map(JSON.stringify).join('\n'),schema);
    }
  }
}

// Renew API function (use with AppScript Triggers)
function requestRenewAPIAccessToken() {
  var url = `${API_URL}oauth/access_token?grant_type=fb_exchange_token&client_id=${CLIENT_ID}&client_secret=${CLIENT_SECRET}&fb_exchange_token=${access_token}`;
  var result = JSON.parse(UrlFetchApp.fetch(encodeURI(url),options).getContentText());
  var options = {
    'method' : 'get'
  };
  console.log(result);
  PropertiesService.getScriptProperties().setProperties({access_token: result["access_token"]},false);
}

// Initial function needed to be run before use
function loadProperties() {
  PropertiesService.getScriptProperties().setProperties({access_token: INITIAL_ACCESS_TOKEN},false);
}

// Test function (currently use for segmentize report request)
function test() {
  //requestReport("max");
  var since = new Date(2021,8,7);
  var until = new Date(2021,8,8);
  requestReportByTimeRange(since,until);
}
