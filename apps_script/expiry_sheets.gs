/**
 * Expiry agent -> Google Sheets (Elma, 18 Nov 2026). Replaces claude_check_sheet.gs.
 *
 * After the midday run, GitHub (scripts/finalize.py) posts once a day:
 *   {token, action: "daily", date: "YYYY-MM-DD", removed: <GitHub + Claude removed today>,
 *    manual_rows: [[listing_id, position, url, claude_status], ...]}
 * and this script
 *   1. adds the manual_rows under the last row of the "Manual check" tab (this spreadsheet), columns A-D only;
 *      column E "expired or not?" is Elma's and is never written;
 *   2. writes the removed count to row 16 ("Checked on Github") of the "Expired roles" tab in
 *      "PositionOps & Commercial team Data", in the column whose row-1 header ends with the date ("Wednesday 18/11").
 *      Saturday and Sunday have no column: their numbers are added to the following Monday's cell
 *      (Monday's cell = Saturday + Sunday + Monday).
 * The same date posted twice (a re-run) does not add the Manual check rows again and does not double the count.
 *
 * Set up (Elma): in the "Expired jobs checker" spreadsheet, Extensions > Apps Script, replace the old code with this
 * file, save, then Deploy > Manage deployments > edit (pencil) > Version: New version > Deploy. Google asks once to
 * allow access to your spreadsheets (it now also writes to the data table). The URL and TOKEN stay the same.
 */
var MANUAL_TAB = 'Manual check';
var DATA_SPREADSHEET_ID = '1bGozWzF1fTS_NdGgULHqnGONdBpGOkTYaQJhcuNbb7Q';  // PositionOps & Commercial team Data
var DATA_TAB = 'Expired roles';
var DATA_ROW = 16;  // "Checked on Github"
var KEEP_DAYS = 21;

function doPost(e) {
  var d = JSON.parse(e.postData.contents);
  var props = PropertiesService.getScriptProperties();
  var token = props.getProperty('TOKEN');
  if (!token || d.token !== token) return out_({ok: false, error: 'bad token'});
  if (d.action !== 'daily' || !/^\d{4}-\d{2}-\d{2}$/.test(d.date || '')) return out_({ok: false, error: 'bad request'});

  var lock = LockService.getScriptLock();
  lock.waitLock(30000);
  try {
    var result = {ok: true, date: d.date};

    // 1. Manual check rows (once per date)
    var doneKey = 'manual_' + d.date;
    if (props.getProperty(doneKey)) {
      result.manual = 'already added for this date';
    } else {
      var rows = (d.manual_rows || []).map(function (r) {
        return r.slice(0, 4).map(text_);
      });
      if (rows.length) {
        var sh = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(MANUAL_TAB);
        if (!sh) return out_({ok: false, error: 'no ' + MANUAL_TAB + ' tab'});
        var last = lastRowInA_(sh);
        if (sh.getMaxRows() < last + rows.length) sh.insertRowsAfter(sh.getMaxRows(), last + rows.length - sh.getMaxRows());
        sh.getRange(last + 1, 1, rows.length, 4).setValues(rows);
      }
      props.setProperty(doneKey, '1');
      result.manual = rows.length + ' rows added';
    }

    // 2. Removed count -> data table (weekend days go to the next Monday)
    props.setProperty('removed_' + d.date, String(Number(d.removed) || 0));
    var target = targetDay_(d.date);
    var total = 0;
    for (var i = 0; i < 3; i++) {  // the target day and up to two days before it (Sat, Sun for a Monday)
      var day = addDays_(target, -i);
      if (i > 0 && targetDay_(day) !== target) break;
      total += Number(props.getProperty('removed_' + day) || 0);
    }
    var data = SpreadsheetApp.openById(DATA_SPREADSHEET_ID).getSheetByName(DATA_TAB);
    var col = findColumn_(data, target);
    if (col) {
      data.getRange(DATA_ROW, col).setValue(total);
      result.data_table = total + ' written to ' + data.getRange(DATA_ROW, col).getA1Notation() + ' (' + target + ')';
    } else {
      result.data_table = 'no column for ' + target + ' yet; ' + total + ' kept and written when that column exists';
    }

    cleanup_(props, d.date);
    return out_(result);
  } finally {
    lock.releaseLock();
  }
}

// Weekend dates count for the following Monday.
function targetDay_(iso) {
  var dow = new Date(iso + 'T12:00:00Z').getUTCDay();  // 0 Sun .. 6 Sat
  if (dow === 6) return addDays_(iso, 2);
  if (dow === 0) return addDays_(iso, 1);
  return iso;
}

function addDays_(iso, n) {
  var t = new Date(iso + 'T12:00:00Z');
  t.setUTCDate(t.getUTCDate() + n);
  return t.toISOString().slice(0, 10);
}

// Row 1 headers look like "Wednesday 18/11", newest week on the left. Only the first 18 columns (about the newest
// three weeks) are searched and the weekday must match too, so a column from an earlier year is never used.
var DAYS = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
function findColumn_(sh, iso) {
  var want = DAYS[new Date(iso + 'T12:00:00Z').getUTCDay()] + ' ' + iso.slice(8, 10) + '/' + iso.slice(5, 7);
  var header = sh.getRange(1, 1, 1, Math.min(18, sh.getLastColumn())).getDisplayValues()[0];
  for (var c = 1; c < header.length; c++) {
    if (String(header[c]).replace(/\s+/g, ' ').trim() === want) return c + 1;
  }
  return 0;
}

function lastRowInA_(sh) {
  var vals = sh.getRange(1, 1, sh.getMaxRows(), 1).getValues();
  for (var r = vals.length - 1; r >= 0; r--) if (String(vals[r][0]).trim() !== '') return r + 1;
  return 1;
}

function cleanup_(props, today) {
  var all = props.getProperties();
  var cutoff = addDays_(today, -KEEP_DAYS);
  Object.keys(all).forEach(function (k) {
    var m = k.match(/^(manual|removed)_(\d{4}-\d{2}-\d{2})$/);
    if (m && m[2] < cutoff) props.deleteProperty(k);
  });
}

function text_(v) {  // text only: a value starting with = + - @ is never run as a formula
  v = v === null || v === undefined ? '' : String(v);
  return /^[=+\-@]/.test(v) ? "'" + v : v;
}

function out_(o) {
  return ContentService.createTextOutput(JSON.stringify(o)).setMimeType(ContentService.MimeType.JSON);
}
