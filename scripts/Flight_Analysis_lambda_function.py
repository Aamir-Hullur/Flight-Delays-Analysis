import boto3
import csv
import io

BUCKET_NAME = "flight-delays-data-bucket"
RAW_DATA_KEY = "flight_delays.csv"
PROCESSED_DATA_KEY = "flight_data_pipeline.csv"

def lambda_handler(event, context):
    try:
        s3 = boto3.client('s3')
        print(f"Downloading raw data from S3 bucket: {BUCKET_NAME}, key: {RAW_DATA_KEY}")

        raw_data = obj['Body'].read().decode('utf-8')

        # Read CSV Data
        csv_reader = csv.DictReader(io.StringIO(raw_data))
        processed_rows = []

        scheduled_departure_col = 'ScheduledDeparture'
        delay_minutes_col = 'DelayMinutes'
        delay_reason_col = 'DelayReason'

        for i, row in enumerate(csv_reader):
            try:
                scheduled_departure = row.get(scheduled_departure_col, "").strip()
                if not scheduled_departure or 'T' not in scheduled_departure:
                    continue

                delay_minutes = row.get(delay_minutes_col, "").strip()
                if not delay_minutes.isdigit():
                    continue

                delay_minutes = int(delay_minutes)
                if delay_minutes <= 0:
                    row[delay_reason_col] = 'On Time'

                row['IsDelayed'] = 'True' if delay_minutes > 0 else 'False'

                hour = int(scheduled_departure.split('T')[1][:2])
                if 5 <= hour < 12:
                    row['TimeOfDay'] = 'Morning'
                elif 12 <= hour < 17:
                    row['TimeOfDay'] = 'Afternoon'
                elif 17 <= hour < 21:
                    row['TimeOfDay'] = 'Evening'
                else:
                    row['TimeOfDay'] = 'Night'

                if delay_minutes <= 0:
                    row['DelayCategory'] = 'On Time'
                elif delay_minutes <= 15:
                    row['DelayCategory'] = 'Short Delay'
                elif delay_minutes <= 60:
                    row['DelayCategory'] = 'Moderate Delay'
                else:
                    row['DelayCategory'] = 'Long Delay'

                processed_rows.append(row)

            except Exception as e:
                continue


        output_buffer = io.StringIO()
        fieldnames = csv_reader.fieldnames + ['IsDelayed', 'TimeOfDay', 'DelayCategory']
        csv_writer = csv.DictWriter(output_buffer, fieldnames=fieldnames)
        csv_writer.writeheader()
        csv_writer.writerows(processed_rows)

        print(f"Uploading processed data to S3 bucket: {BUCKET_NAME}, key: {PROCESSED_DATA_KEY}")
        s3.put_object(Bucket=BUCKET_NAME, Key=PROCESSED_DATA_KEY, Body=output_buffer.getvalue())


        return {
            'statusCode': 200,
            'body': f"Data processing complete. Processed data saved to: s3://{BUCKET_NAME}/{PROCESSED_DATA_KEY}"
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Pipeline execution failed: {str(e)}"
        }