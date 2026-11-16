/**
 * Writes the GitHub Claude check answers to the "Claude check" tab of the "Expired jobs checker" spreadsheet.
 * Set up once (Elma): in the spreadsheet, Extensions > Apps Script, paste this file, then
 *   Project settings > Script properties: TOKEN = a long random text (the same text goes in the GitHub secret
 *   SHEET_WEBHOOK_TOKEN), Deploy > New deployment > Web app (execute as: me, who has access: anyone),
 *   and put the web app URL in the GitHub secret SHEET_WEBHOOK_URL.
 * It only ever appends rows to the "Claude check" tab (created on first use); it never touches other tabs.
 */
var TAB = 'Claude check';

function doPost(e) {
  var d = JSON.parse(e.postData.contents);
  var token = PropertiesService.getScriptProperties().getProperty('TOKEN');
  if (!token || d.token !== token) return out_({ok: false, error: 'bad token'});
  if (d.tab !== TAB) return out_({ok: false, error: 'only the ' + TAB + ' tab can be written'});
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sh = ss.getSheetByName(TAB) || ss.insertSheet(TAB);
  if (sh.getLastRow() === 0) {
    sh.appendRow(d.header);
    sh.setFrozenRows(1);
  }
  var rows = (d.rows || []).map(function (r) {
    return r.map(function (v) {  // text only: a value starting with = + - @ is never run as a formula
      v = v === null || v === undefined ? '' : String(v);
      return /^[=+\-@]/.test(v) ? "'" + v : v;
    });
  });
  if (rows.length) sh.getRange(sh.getLastRow() + 1, 1, rows.length, rows[0].length).setValues(rows);
  return out_({ok: true, appended: rows.length});
}

function out_(o) {
  return ContentService.createTextOutput(JSON.stringify(o)).setMimeType(ContentService.MimeType.JSON);
}
