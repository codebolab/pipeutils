==================================
Getting Started
==================================
Drive API requires OAuth2.0 for authentication.

Setup steps you need to complete before you can use this library:

1. If you don't already have a Google account `sign up <https://www.google.com/accounts>`_.
2. Open your target Apps Script in the editor and select Resources > Developers Console Project.: `Google API Console <https://console.developers.google.com/>`_.
3. In the dialog that opens, click on the blue link (that starts with "Apps Script Execution API Quickstart Target") at the top to open the console project associated with your script.
4. In the main dashboard, find the Use Google APIs panel. Click `Enable and manage APIs <https://developers.google.com/drive/api/v3/quickstart/python>`_.
5. In the search bar under the Google APIs tab, enter "Google Apps Script Execution API". Click the same name in the list that appears. In the new tab that opens, click Enable API.
6. Click Credentials in the left sidebar.
7. Select the Credentials tab, click the Add credentials button and select OAuth 2.0 client ID.
8. Select the application type Other, enter the name "Google Apps Script Execution API Quickstart", and click the Create button.
9. Click OK to dismiss the resulting dialog.
10. Click the  (Download JSON) button to the right of the client ID.
11. Move this file to your ~/.pipeutils directory and rename it gdrive.conf.