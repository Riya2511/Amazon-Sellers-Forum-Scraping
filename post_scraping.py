import json
import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from datetime import datetime
import mysql.connector
import traceback

# Choose from ["lastActivityTime", "createdAt", "totalViews", "totalVotes"]
SORT_BY = ["lastActivityTime", "createdAt", "totalViews", "totalVotes"]

# Choose one from ["allTime", "pastDay", "pastWeek", "pastMonth", "threeMonths", "pastYear"]
DATE_RANGE = "pastYear"

# CHOOSE from ['Account Health', 'Account Setup', 'Community Connections', 'Create and Manage Listings', 'Fulfill Orders', 'Grow Your Business', 'Manage Buyer Experience', 'Manage Inventory', 'Manage Your Brand', 'News and Announcements', 'Product Safety and Compliance']
CATEGORIES = ['Account Health', 'Account Setup', 'Community Connections', 'Create and Manage Listings', 'Fulfill Orders', 'Grow Your Business', 'Manage Buyer Experience', 'Manage Inventory', 'Manage Your Brand', 'News and Announcements', 'Product Safety and Compliance']

# this is the waiting time before the page scrolls. So before every scroll. 
# So total time taken for loading a page is amount of scrolls * WAIT_TIME
# To make the load faster, you can reduce this time, but make sure it loads the page in that less time. 
WAIT_TIME = 3

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

def generate_all_page_urls(base_url):
    sort_options = ["lastActivityTime", "createdAt", "totalViews", "totalVotes"]
    date_range_options = ["allTime", "pastDay", "pastWeek", "pastMonth", "threeMonths", "pastYear"]
    categories = {
        "Account Health": "amzn1.spce.category.8b1ad9d6",
        "Account Setup": "amzn1.spce.category.8b1ad26a",
        "Community Connections": "amzn1.spce.category.8b1ad9e8",
        "Create and Manage Listings": "amzn1.spce.category.8b1ad436",
        "Fulfill Orders": "amzn1.spce.category.8b1ad526",
        "Grow Your Business": "amzn1.spce.category.8b1ad7ec",
        "Manage Buyer Experience": "amzn1.spce.category.8b1ad8dc",
        "Manage Inventory": "amzn1.spce.category.8b1ad60c",
        "Manage Your Brand": "amzn1.spce.category.8b1ad6fc",
        "News and Announcements": "amzn1.spce.category.8b1ad9d2",
        "Product Safety and Compliance": "amzn1.spce.category.8b1ad9e4"
    }
    if not set(SORT_BY) <= set(sort_options):
        print("Error: Please update the SORT_BY parameter correctly. Choose from the following list: ", sort_options)
        return {}
    if DATE_RANGE not in date_range_options: 
        print("Error: Please update the DATE_RANGE parameter correctly. Choose one from the following list: ", date_range_options)
        return {}
    if not set(CATEGORIES) <= categories.keys(): 
        print("Error: Please update the CATEGORIES parameter correctly. Choose from the following list: ", ", ".join(categories.keys()))
        return {}
    url_dict = {}
    for sort_value in SORT_BY:
        url_dict[sort_value] = {}
        for category_name in CATEGORIES:
            category_id = categories.get(category_name)
            if not category_id:
                print(f"Warning: '{category_name}' is not a valid category name.")
                continue
            url = f"{base_url}?sortBy={sort_value}&dateRange={DATE_RANGE}&replies=repliesAll&contentType=ALL&categories[]={category_id}"
            url_dict[sort_value][category_name] = url
    return url_dict

def load_page_with_selenium(url, driver, wait_time=5):
    driver.get(url)
    time.sleep(wait_time)
    previous_content_length = 0
    no_change_count = 0
    while True:
        driver.find_element(By.TAG_NAME, "body").send_keys(Keys.END)
        time.sleep(WAIT_TIME)
        
        current_content_length = len(driver.page_source)
        
        if current_content_length == previous_content_length:
            no_change_count += 1
            if no_change_count >= 3:
                break
        else:
            no_change_count = 0
        previous_content_length = current_content_length
    page_source = driver.page_source
    return page_source

def scrape_data(html_source):
    try:
        soup = BeautifulSoup(html_source, 'html.parser')
        all_posts = soup.find("div", {"data-testid": "searchListing-container"}).find_all("div", {"data-testid": "search-post-layout"})
        post_to_upload = []
        for post in all_posts:
            post_details = {}
            header = post.find("div", {"data-testid": "header"})
            thread_link = header.find("a")
            post_details['thread_id'] = (thread_link['href']).split('/')[-1] if thread_link and thread_link.has_attr('href') else None
            post_details['thread_title'] = thread_link.text.strip() if thread_link else None
            post_details['seller_id'] = header.find("div").find("div").find("div").text.replace('"', "").replace("by", "").strip()
            posted_at_str = header.find("time")['datetime'] if header.find("time") and header.find("time").has_attr("datetime") else None
            if posted_at_str:
                try:
                    post_details['posted_at'] = datetime.fromisoformat(posted_at_str.replace('Z', '+00:00'))
                except Exception:
                    post_details['posted_at'] = posted_at_str
            else:
                post_details['posted_at'] = None

            post_body = post.find("div", {"data-testid": "content-expander"}).find("div").find_all("p")
            post_details['post_body'] = " ".join([p.text for p in post_body])
            last_time = post.find("div", {"data-testid":"last-activity-metric"}).find("time")
            last_time_str = last_time["datetime"] if last_time and last_time.has_attr("datetime") else None
            if last_time_str:
                try:
                    post_details['last_activity_at'] = datetime.fromisoformat(last_time_str.replace('Z', '+00:00'))
                except Exception:
                    post_details['last_activity_at'] = last_time_str
            else:
                post_details['last_activity_at'] = None
            
            post_details['up_votes'] = parse_count(post.find('div', {'data-testid': 'upvote-metric'}).get_text(strip=True).split()[0])
            post_details['down_votes'] = parse_count(post.find('div', {'data-testid': 'downvote-metric'}).get_text(strip=True).split()[0])
            post_details['view_count'] = parse_count(post.find('div', {'data-testid': 'view-metric'}).get_text(strip=True).split()[0])
            post_details['reply_count'] = parse_count(post.find('div', {'data-testid': 'reply-metric'}).get_text(strip=True).split()[0])
            post_to_upload.append(post_details)
        return post_to_upload
    except Exception as e:
        print(f"Error scraping data: {e}")
        return None

def upload_scraped_data(conn, table, data):
    cursor = conn.cursor()
    if not data:
        return
    columns = ', '.join(data[0].keys())
    placeholders = ', '.join(['%s'] * len(data[0]))
    update_clause = ', '.join([f"{col}=VALUES({col})" for col in data[0].keys() if col != 'thread_id'])
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

def main(url, category, sorted_by, driver):
    print(url)
    print("Loading page and scrolling till the bottom... This might take a while... It loads almost 1K-1.5K records.")
    page_source = load_page_with_selenium(url, driver)
    print("Done with the page load.")
    scraped_data = scrape_data(page_source)
    for i in scraped_data:
        i['category'] = category
        i['sorted_by'] = sorted_by
    upload_scraped_data(conn, 'stg_amz_seller_forums_post', scraped_data)
    print("Done scraping! :)")

# Example usage
if __name__ == "__main__":
    base_url = "https://sellercentral.amazon.com/seller-forums/discussions"
    urls = generate_all_page_urls(base_url)
    driver = setup_headless_driver()
    conn = connect_to_sql()
    for sorted_by, categories in urls.items(): 
        for category, url in categories.items():
            main(url, category, sorted_by, driver, conn)
            break
        break
    driver.quit()
    conn.close()
