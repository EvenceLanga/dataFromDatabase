from flask import Flask, make_response, render_template, request, Response, redirect, url_for
import mysql.connector
from mysql.connector import Error
import pandas as pd

app = Flask(__name__)

def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name,
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: {err}")
    return connection

def execute_query(connection, query, params=None):
    try:
        cursor = connection.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        records = cursor.fetchall()
        df = pd.DataFrame(records, columns=[col[0] for col in cursor.description])
        cursor.close()
        return df
    except Error as err:
        print(f"Error executing query: {err}")
        return None
############################################
#########################################
 #EXPORTS
###################################
@app.route('/export_csv')
def export_csv():
    connection = create_db_connection(
        host_name='#################',
        user_name='#################',
        user_password='#################',
        db_name='##############',
    )

    if connection:
        query = """
        SELECT devices.olt_name AS 'olt_name', 
               COUNT(*) AS 'Totals', 
               COUNT(CASE WHEN alerts = 'light levels are null with no alert' THEN 1 END) AS 'Light levels are null with no alert',
               COUNT(CASE WHEN alerts = 'Possible Fibre Fault' THEN 1 END) AS 'Possible Fibre Fault',
               COUNT(CASE WHEN alerts = 'Possible Power Down' THEN 1 END) AS 'Possible Power Down',
               COUNT(CASE WHEN alerts = 'RX is abnormal' THEN 1 END) AS 'RX is abnormal',
               COUNT(CASE WHEN alerts = 'Up' THEN 1 END) AS 'Up'
        FROM devices
        WHERE devices.live = 1
        GROUP BY devices.olt_name
        ORDER BY devices.olt_name;
        """

        result = execute_query(connection, query)

        if result is not None:
            csv_data = result.to_csv(index=False)
            return Response(
                csv_data,
                mimetype="text/csv",
                headers={"Content-disposition": "attachment; filename=exported_data.csv"},
            )
        else:
            return "No data found."

        connection.close()
    else:
        return "Error connecting to the database."


@app.route('/export_alerts_details_csv/<alerts>')
def export_alerts_details_csv(alerts):
    olt_name = request.args.get('olt_name')

    if olt_name:
        connection = create_db_connection(
            host_name='###########',
            user_name='##############',
            user_password='#################',
            db_name='###################',
        )

        if connection:
            details_query = """
    SELECT p.asset_reference AS 'FSAN',
	CONCAT(c.first_name, ' ', c.last_name) AS 'Customer',
	c.mobile_number AS 'Mobile',
	c.email AS 'Email',
	p.full_address AS 'Address',
        d.alerts AS 'Status',
        d.tx_light_level AS 'TX Light Levels',
        d.sfp_light_level AS 'SFP Light Levels',
        d.rx_light_level AS 'RX Light Levels',
        d.olt_name AS 'OLT Name',
        d.name AS 'ONU Port',
        p.latitude AS 'Latitude',
        p.longitude AS 'Longitude'
    FROM services AS s
    LEFT JOIN premises AS p ON s.premise_id = p.aex_id
    LEFT JOIN customers AS c ON s.customer_id = c.aex_id
    LEFT JOIN devices AS d ON d.fsan = p.asset_reference
    WHERE
    s.on_network = TRUE
    AND
    s.status_id IN (2, 5, 9, 16)
    AND
    (s.deleted_at > NOW() OR s.deleted_at IS NULL)
    AND d.alerts = %s
    AND d.olt_name = %s
    """

            try:
                cursor = connection.cursor()

                cursor.execute(details_query, (alerts, olt_name))
                details = cursor.fetchall()

                cursor.close()
            except Error as err:
                print(f"Error executing query: {err}")
                details = None

            if details:
                formatted_details = []
                for detail in details:
                    formatted_detail = {
                        'fsan': detail[0],
			            'customer': detail[1],
                        'mobile': detail[2],
                        'email': detail[3],
                        'address': detail[4],
                        'status': detail[5],
                        'tx_light_levels': detail[6],
                        'sfp_light_levels': detail[7],
                        'rx_light_levels': detail[8],
                        'olt_name': detail[9],
                        'onu_port': detail[10],
                        'latitude': detail[11],
                        'longitude': detail[12]
                    }
                    formatted_details.append(formatted_detail)

                df = pd.DataFrame(formatted_details)
                csv_data = df.to_csv(index=False)

                response = Response(csv_data, mimetype="text/csv")
                response.headers['Content-Disposition'] = f'attachment; filename=exported_alerts_details.csv'

                return response
            else:
                return "No data found."

            connection.close()
        else:
            return "Error connecting to the database."
    else:
        return "olt_name parameter not provided."


@app.route('/export_alerts_csv')
def export_alerts_csv():
    olt_name = request.args.get('olt_name')

    if olt_name:
        connection = create_db_connection(
            host_name='###########',
            user_name='##############',
            user_password='#################',
            db_name='###################',
        )

        if connection:
            alerts_query = """
            SELECT alerts, COUNT(*) AS totals
            FROM devices
            WHERE olt_name = %s
            AND live = 1
            GROUP BY alerts
            """

            result = execute_query(connection, alerts_query, (olt_name,))

            if result is not None:
                csv_data = result.to_csv(index=False)
                response = make_response(csv_data)
                response.headers['Content-Disposition'] = f'attachment; filename=exported_alerts_data.csv'
                response.headers['Content-Type'] = 'text/csv'
                return response
            else:
                return "No data found."

            connection.close()
        else:
            return "Error connecting to the database."
    else:
        return "olt_name parameter not provided."

############################################
#########################################
 #EXPORTS
###################################

@app.route('/')
def display_data():
     connection = create_db_connection(
            host_name='###########',
            user_name='##############',
            user_password='#################',
            db_name='###################',
        )

    if connection:
        query1 = """
        SELECT
            GROUP_CONCAT(DISTINCT
                CONCAT(
                    'COUNT(CASE WHEN alerts = ''',
                    alerts,
                    ''' THEN 1 END) AS \'',
                    alerts, '\''
                )
            ) as dynamic_columns
        FROM devices;
        """

        query2 = """
        SELECT devices.olt_name AS 'olt_name', 
               COUNT(*) AS 'Totals', 
               COUNT(CASE WHEN alerts = 'light levels are null with no alert' THEN 1 END) AS 'Light levels are null with no alert',
               COUNT(CASE WHEN alerts = 'Possible Fibre Fault' THEN 1 END) AS 'Possible Fibre Fault',
               COUNT(CASE WHEN alerts = 'Possible Power Down' THEN 1 END) AS 'Possible Power Down',
               COUNT(CASE WHEN alerts = 'RX is abnormal' THEN 1 END) AS 'RX is abnormal',
               COUNT(CASE WHEN alerts = 'Up' THEN 1 END) AS 'Up',
               devices.olt_name AS 'Locate To'
        FROM devices
        WHERE devices.live = 1
        GROUP BY devices.olt_name
        ORDER BY devices.olt_name;
        """

        result1 = execute_query(connection, query1)

        if result1 is not None and not result1.empty:
            dynamic_columns = result1['dynamic_columns'][0]
            query2 = query2.format(dynamic_columns=dynamic_columns)
            result2 = execute_query(connection, query2)

            if result2 is not None:
                result2['olt_name'] = result2['olt_name'].apply(
                    lambda olt_name: f'<a href="/detail?olt_name={olt_name}">{olt_name}</a>'
                )


                columns_to_display = [col for col in result2.columns if col != 'alerts']
                result2_filtered = result2[columns_to_display]

                olt_name = request.args.get('olt_name')  # Retrieve olt_name from request args

                alerts_query = """
                SELECT
                    alerts,
                    COUNT(*) AS totals
                FROM
                    devices
                WHERE
                    live = 1
                    AND olt_name = %s
                GROUP BY
                    alerts
                """

                alerts_count = execute_query(connection, alerts_query, (olt_name,))  # Provide a value for %s

                if alerts_count is not None:
                    return render_template('index.html', data=result2_filtered.to_html(classes="table table-bordered", header="true", index=False, escape=False), alert_counts=alerts_count.to_html(classes="table table-bordered", header="true", index=False, escape=False))
                else:
                    data = "No alerts data found."
            else:
                data = "No data found."
        else:
            data = "No data found."

        connection.close()
    else:
        data = "Error connecting to the database."

    return render_template('index.html', data=data)

# The rest of your code remains the same
@app.route('/detail')
def olt_detail():
    olt_name = request.args.get('olt_name')
    details_query = """
    SELECT fsan, added, alerts
    FROM devices 
    WHERE olt_name = %s 
    AND live = 1
    """

    alerts_query = """
    SELECT alerts, COUNT(*) AS totals
    FROM devices
    WHERE olt_name = %s
    AND live = 1
    GROUP BY alerts
    """

    try:
      connection = create_db_connection(
            host_name='###########',
            user_name='##############',
            user_password='#################',
            db_name='###################',
        )

        cursor = connection.cursor()

        cursor.execute(details_query, (olt_name,))
        details = cursor.fetchall()

        cursor.execute(alerts_query, (olt_name,))
        alert_counts = cursor.fetchall()

        cursor.close()
        connection.close()
    except Error as err:
        print(f"Error executing query: {err}")
        details = None
        alert_counts = None

    if details:
        formatted_details = []
        for detail in details:
            formatted_detail = {
                'fsan': detail[0],
                'added': detail[1].strftime('%Y-%m-%d %H:%M'),
                'alerts': detail[2],
            }
            formatted_details.append(formatted_detail)

        return render_template('detail.html', olt_name=olt_name, details=formatted_details, alert_counts=alert_counts)
    else:
        return "Details not found."

@app.route('/alerts_details/<alerts>')
def alerts_details(alerts):
    olt_name = request.args.get('olt_name')
    details_query = """
    SELECT p.asset_reference AS 'FSAN',
        p.full_address AS 'Address',
        d.alerts AS 'Status',
        d.tx_light_level AS 'TX Light Levels',
        d.sfp_light_level AS 'SFP Light Levels',
        d.rx_light_level AS 'RX Light Levels',
        d.olt_name AS 'OLT Name',
        d.name AS 'ONU Port',
        p.latitude AS 'Latitude',
        p.longitude AS 'Longitude',
        CONCAT(c.first_name, ' ', c.last_name) AS 'Customer',
        c.mobile_number AS 'Mobile',
        c.email AS 'Email'
    FROM services AS s
    LEFT JOIN premises AS p ON s.premise_id = p.aex_id
    LEFT JOIN customers AS c ON s.customer_id = c.aex_id
    LEFT JOIN devices AS d ON d.fsan = p.asset_reference
    WHERE
    s.on_network = TRUE
    AND
    s.status_id IN (2, 5, 9, 16)
    AND
    (s.deleted_at > NOW() OR s.deleted_at IS NULL)
    AND d.alerts = %s
    AND d.olt_name = %s
    """

    try:
        connection = create_db_connection(
            host_name='###########',
            user_name='##############',
            user_password='#################',
            db_name='###################',
        )

        cursor = connection.cursor()

        cursor.execute(details_query, (alerts, olt_name))
        details = cursor.fetchall()

        cursor.close()
        connection.close()
    except Error as err:
        print(f"Error executing query: {err}")
        details = None

    if details:
        formatted_details = []
        for detail in details:
            formatted_detail = {
                'fsan': detail[0],
                'address': detail[1],
                'status': detail[2],
                'tx_light_levels': detail[3],
                'sfp_light_levels': detail[4],
                'rx_light_levels': detail[5],
                'olt_name': detail[6],
                'onu_port': detail[7],
                'latitude': detail[8],
                'longitude': detail[9],
                'customer': detail[10],
                'mobile': detail[11],
                'email': detail[12],
            }
            formatted_details.append(formatted_detail)

        return render_template('alerts_details.html', olt_name=olt_name, alerts=alerts, details=formatted_details)
    else:
        return "Details not found."


if __name__ == '__main__':
    app.run()
