import json
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from datetime import datetime
import mysql.connector
import sys

# CONFIGURATION PARAMETERS
BATCH_SIZE = 100                # Number of threads to fetch at once
STORE_FREQUENCY = 15            # Store to database after processing this many threads
MAX_RETRIES = 3                 # Maximum number of retries for driver setup
RETRY_DELAY = 5                 # Seconds to wait between retries
PAGE_LOAD_WAIT_TIME = 5         # Seconds to wait for page to load

config_path='db_config_leadsniper.json'
with open(config_path, 'r') as f:
    config = json.load(f)

def setup_headless_driver():
    options = webdriver.ChromeOptions()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)    
    driver = webdriver.Chrome(options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def connect_to_sql():
    conn = mysql.connector.connect(
        host=config['host'],
        user=config['user'],
        password=config['password'],
        database=config['database']
    )
    return conn

def parse_count(s):
    s = s.strip().upper()
    if s.endswith('K'):
        return int(float(s[:-1]) * 1_000)
    elif s.endswith('M'):
        return int(float(s[:-1]) * 1_000_000)
    elif s.endswith('B'):
        return int(float(s[:-1]) * 1_000_000_000)
    else:
        return int(s)

def fetch_all_unprocessed_threads(base_url, batch_size=BATCH_SIZE):
    conn = connect_to_sql()
    cursor = conn.cursor()
    sql = """
        SELECT DISTINCT p.thread_id 
        FROM stg_amz_seller_forums_post p
        LEFT JOIN stg_amz_seller_forums_comments c ON p.thread_id = c.thread_id
        WHERE c.thread_id IS NULL AND p.thread_id IS NOT NULL
        LIMIT %s
    """
    try:
        cursor.execute(sql, (batch_size,))
        results = cursor.fetchall()
        urls = []
        for row in results:
            thread_id = row[0]
            urls.append(f"{base_url}{thread_id}")
        cursor.close()
        conn.close()
        return urls
    except Exception as e:
        print(f"Error fetching unprocessed threads: {e}")
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
        return []

def load_page_with_selenium(url, driver, wait_time=PAGE_LOAD_WAIT_TIME):
   driver.get(url)
   time.sleep(wait_time)
   
   clicked_buttons = set()
   while True:
       reply_buttons = driver.find_elements(By.CSS_SELECTOR, '[data-testid="show-replies"]')
       new_buttons = [btn for btn in reply_buttons if btn not in clicked_buttons]
       if not new_buttons:
           break
       for button in new_buttons:
           try:
               driver.execute_script("arguments[0].click();", button)
               clicked_buttons.add(button)
               time.sleep(1)
           except:
               pass
       time.sleep(2)
   
   return driver.page_source

def scrape_data(html_source, thread_id):
    soup = BeautifulSoup(html_source, 'html.parser')
    all_comments = soup.find_all("div", {"data-testid": "reply-post-layout"})
    comments_to_upload = []
    count = 1
    for comm in all_comments:
        comm_data = {}
        comm_data["comment_id"] = thread_id + f'-{count}'
        comm_data["thread_id"] = thread_id
        header = comm.find("div", {'data-testid' : "header"})
        comm_data['commented_by'] = header.find("h5").text.strip()
        posted_at_str = header.find("time")['datetime'] if header.find("time") and header.find("time").has_attr("datetime") else None
        if posted_at_str:
            try:
                comm_data['commented_at'] = datetime.fromisoformat(posted_at_str.replace('Z', '+00:00'))
            except Exception:
                comm_data['commented_at'] = posted_at_str
        else:
            comm_data['commented_at'] = None
        comment_body = comm.find("div", {"data-testid": "post-content"}).find_all("p")
        comm_data['comment_body'] = " ".join([p.text for p in comment_body])
        vote_container_spans = comm.find("div", {"data-testid": "vote-container"}).find_all('span')
        if len(vote_container_spans) < 2:
            comm_data['up_votes'], comm_data["down_votes"] = 0, 0
        else:
            comm_data['up_votes'] = parse_count(vote_container_spans[0].text)
            comm_data['down_votes'] = parse_count(vote_container_spans[1].text)
        comments_to_upload.append(comm_data)
        count += 1
    return comments_to_upload

def upload_scraped_data(table, data):
    if not data:
        return
    
    conn = connect_to_sql()
    cursor = conn.cursor()
    
    columns = ', '.join(data[0].keys())
    placeholders = ', '.join(['%s'] * len(data[0]))
    update_clause = ', '.join([f"{col}=VALUES({col})" for col in data[0].keys() if col != 'thread_id' or col != 'comment_id'])

    sql = f"""
        INSERT INTO {table} ({columns})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {update_clause}
    """
    values = [tuple(item.values()) for item in data]
    cursor.executemany(sql, values)
    conn.commit()
    cursor.close()
    conn.close()

def process_url_with_retry(url, driver):
    for attempt in range(MAX_RETRIES):
        try:
            page_source = load_page_with_selenium(url, driver)
            return scrape_data(page_source, url.split('/')[-1])
        except Exception as e:
            print(f"Error processing {url}, attempt {attempt+1}/{MAX_RETRIES}: {e}")
            if attempt < MAX_RETRIES - 1:
                try:
                    driver.quit()
                except:
                    pass
                time.sleep(RETRY_DELAY)
                driver = setup_headless_driver()
            else:
                print(f"Failed to process {url} after {MAX_RETRIES} attempts")
                return []
    return []

def main():
    base_url = "https://sellercentral.amazon.com/seller-forums/discussions/t/"
    driver = setup_headless_driver()
    
    try:
        while True:  # Continue until no more unprocessed threads
            urls = fetch_all_unprocessed_threads(base_url)
            
            # Exit loop if no more unprocessed threads
            if not urls:
                print("No more unprocessed threads found. Exiting.")
                break
                
            all_comments = []
            
            for i, url in enumerate(urls):
                print(f"Processing {i+1}/{len(urls)}: {url}")
                
                comments = process_url_with_retry(url, driver)
                all_comments.extend(comments)
                
                if (i + 1) % STORE_FREQUENCY == 0 and all_comments:
                    print(f"Storing batch of {len(all_comments)} comments")
                    upload_scraped_data('stg_amz_seller_forums_comments', all_comments)
                    all_comments = []
            
            if all_comments:
                print(f"Storing final batch of {len(all_comments)} comments")
                upload_scraped_data('stg_amz_seller_forums_comments', all_comments)
            
            print(f"Completed processing batch of {len(urls)} threads. Fetching next batch...")
            
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
