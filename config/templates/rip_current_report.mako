<!DOCTYPE html>

<html lang="en">
    <head>
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <link href="http://howsthebeach.org/static/css/bootstrap/css/bootstrap.min.css" rel="stylesheet">
      <link href=http://howsthebeach.org//static/css/bootstrap/css/css/bootstrap-theme.min.css" rel="stylesheet">

      <title>Folly Beach RIP Current Alert</title>
    </head>
    <body>
        <style>
              .high_level {
                background-color: #ff3633;
              }
              .medium_level {
                background-color: #fff45c;
              }

        </style>
        <div class="container">
            <div class="row">
              <div class="col-xs-12">
                <h1>Folly Beach RIP Current Alert</h1>
                <h2>Prediction for: ${prediction_date}</h2>
              </div>
            </div>
            </br>
            <div class = "row">
            </div>
            <div class="row">
                <table class="table table-bordered">
                    <tr>
                        <th>Site</th>
                        <th>Date</th>
                        <th>Alert</th>
                        <th>Location</th>
                    </tr>
                    % for site_data in rip_current_sites:
                        %if site_data['level'].lower() == 'high':
                        <tr class="high_level">
                        %elif site_data['level'].lower() == 'moderate':
                        <tr class="medium_level">
                        %else:
                        <tr>
                        %endif
                            <td>${site_data['site_description']}</td>
                            <td>${site_data['date']}</td>
                            <td>${site_data['level']}</td>
                            <td>${site_data['location']}</td>
                        </tr>
                    % endfor
                </table>
            </div>

        </div>
    </body>
</html>
