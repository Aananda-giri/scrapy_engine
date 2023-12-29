# Keep alive for replit

from flask import Flask, send_file
from threading import Thread
import os, json
from flask import jsonify
import csv
import gzip

app = Flask('')


@app.route('/')
def home():
  return "Bot is running!"


def save_data(file_name='nepali_dataset.csv'):
  files = os.listdir()
  nepali_dataset = []
  for file in files:
    if file.endswith('.json'):
      try:
        with open(file, 'r') as f:
          data = json.load(f)

          for d in data:
            if type(d) == dict:
              if 'paragraph' in d.keys():
                nepali_dataset.append(d['paragraph'])
      except:
        pass
  # save dataset in a csv file
  with open(file_name, 'w', newline='') as csv_file:
    # Create a CSV writer object
    csv_writer = csv.writer(csv_file)

    # Write the data to the CSV file
    csv_writer.writerows(nepali_dataset)


def compress_file(input_file_path="nepali_dataset.csv",
                  output_file_path="nepali_dataset.csv.gz"):
  # Step 3: Compress the CSV file
  with open(input_file_path, 'rb') as csv_file:
    with gzip.open(output_file_path, 'wb') as compressed_file:
      compressed_file.writelines(csv_file)


@app.route('/data')
def data():
  save_data('nepali_dataset.csv')
  compress_file(input_file_path="nepali_dataset.csv",
                output_file_path="nepali_dataset.csv.gz")

  # Use the send_file function to send the large file as a response
  return send_file("nepali_dataset.csv.gz",
                   as_attachment=True,
                   download_name='nepali_news_dataset.csv.gz',
                   mimetype='application/gzip')


def run():
  app.run(host='0.0.0.0', port=8080)


def keep_alive():
  server = Thread(target=run)
  server.start()


