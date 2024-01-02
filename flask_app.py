# Keep alive for replit

from flask import Flask, send_file, render_template, redirect, url_for
from threading import Thread
import os, json
from flask import jsonify
from datetime import datetime
import csv
import gzip


app = Flask('')


@app.route('/')
def home():
    # Get file size and date of creation
    original_file_path = "nepali_news_dataset.csv"
    compressed_file_path = "nepali_news_dataset.csv.gz"
    
    compressed_file_size_mb = round(os.path.getsize(compressed_file_path) / (1024 * 1024),3) if os.path.exists(compressed_file_path) else 0
    original_file_size_mb = round(os.path.getsize(original_file_path) / (1024 * 1024),3) if os.path.exists(original_file_path) else 0

    sites_crawled = set([file.split('_')[0].split('.json')[0] for file in os.listdir() if (file.endswith('.json')) and (file not in ['test.json', 'news_start_urls copy.json', 'news_start_urls.json'])])
    # sites_crawled = {'onlinekhabar.com', 'onlinekhabar2.com', 'onlinekhabar3.com'}
    creation_date = datetime.utcfromtimestamp(os.path.getctime(compressed_file_path)).strftime('%Y-%m-%d %H:%M:%S') if os.path.exists(compressed_file_path) else None

    # Render HTML template with file information and buttons
    return render_template('data_template.html',
                            file_size_compressed=compressed_file_size_mb,
                            file_size_original=original_file_size_mb,
                            creation_date=creation_date,
                            sites_crawled=sites_crawled)
    # save_data('nepali_dataset.csv')
    # compress_file(input_file_path="nepali_dataset.csv",
    #               output_file_path="nepali_dataset.csv.gz")

@app.route('/download', methods=['GET'])
def download_file():
  print('downloading...')
  # Use the send_file function to send the large file as a response
  return send_file("nepali_news_dataset.csv.gz",
                   as_attachment=True,
                   download_name='nepali_news_dataset.csv.gz',
                   mimetype='application/gzip')
  # Redirect to the '/data' page
  return redirect(url_for('home'))


@app.route('/update_info', methods=['POST'])
def update_info(file_name='nepali_news_dataset.csv'):
  print('Upadting info...')
  # Remove old files
  if os.path.exists('nepali_news_dataset.csv.gz'):
    os.remove('nepali_news_dataset.csv.gz')
  if os.path.exists('nepali_news_dataset.csv'):
    os.remove('nepali_news_dataset.csv')
  
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
                # nepali_dataset.append(d['paragraph'])
                
                # save dataset in a csv file
                with open(file_name, 'a', newline='') as csv_file:
                  # Create a CSV writer object
                  csv_writer = csv.writer(csv_file)

                  # Write the data to the CSV file
                  csv_writer.writerows(nepali_dataset)
      except:
        pass
  del nepali_dataset  # free up memory
  compress_file()     # compress the file
  # Redirect to the '/data' page
  return redirect(url_for('home'))

def compress_file(input_file_path="nepali_news_dataset.csv",
                  output_file_path="nepali_news_dataset.csv.gz"):
  # Step 3: Compress the CSV file
  with open(input_file_path, 'rb') as csv_file:
    with gzip.open(output_file_path, 'wb') as compressed_file:
      compressed_file.writelines(csv_file)

@app.route('/data', methods=['GET'])
def data():
  return redirect(url_for('home'))

def run():
  app.run(host='0.0.0.0', port=8080)


def keep_alive():
  server = Thread(target=run)
  server.start()


if __name__ == "__main__":
  keep_alive()
  # update_data()