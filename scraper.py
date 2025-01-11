import re
import sqlite3

import requests
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import quote

# Common browser headers
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1'
}


class Database:
    def __init__(self, db_name='bigfinish.db'):
        self.db_name = db_name
        self.conn = None
        self.cursor = None

    def connect(self):
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()

    def close(self):
        if self.conn:
            self.conn.close()

    def create_tables(self):
        self.connect()
        # Create main content table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS content (
                url TEXT PRIMARY KEY,
                title TEXT,
                series TEXT,
                release_date TEXT,
                about TEXT,
                background TEXT,
                production TEXT,
                duration TEXT,
                isbn TEXT,
                cover_url TEXT,
                written_by TEXT,
                narrated_by TEXT,
                series_tag TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Create URLs table with status
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS urls (
                url TEXT PRIMARY KEY,
                visited BOOLEAN DEFAULT FALSE,
                visited_at TIMESTAMP,
                discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        self.conn.commit()
        self.close()

    def add_url(self, url):
        """Add a new URL to the database if it doesn't exist"""
        self.connect()
        self.cursor.execute('''
            INSERT OR IGNORE INTO urls (url, discovered_at)
            VALUES (?, CURRENT_TIMESTAMP)
        ''', (url,))
        self.conn.commit()
        self.close()

    def mark_url_visited(self, url):
        """Mark a URL as visited"""
        self.connect()
        self.cursor.execute('''
            INSERT OR REPLACE INTO urls (url, visited, visited_at)
            VALUES (?, TRUE, CURRENT_TIMESTAMP)
        ''', (url,))
        self.conn.commit()
        self.close()

    def get_all_urls(self):
        """Get all URLs and their visited status"""
        self.connect()
        self.cursor.execute('SELECT url, visited FROM urls')
        urls = {row[0]: bool(row[1]) for row in self.cursor.fetchall()}
        self.close()
        return urls

    def save_content(self, data):
        self.connect()
        self.cursor.execute('''
            INSERT OR REPLACE INTO content 
            (url, title, series, release_date, about, background, 
             production, duration, isbn, written_by, narrated_by, cover_url, series_tag)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            data['url'],
            data['title'],
            data['series'],
            data['release_date'],
            data['about'],
            data['background'],
            data['production'],
            data['duration'],
            data['isbn'],
            data['written_by'],
            data['narrated_by'],
            data['cover_url'],
            data['series_tag']
        ))
        self.conn.commit()
        self.close()

    def return_data_for_url(self, url):
        try:
            self.connect()
            self.cursor.execute('''
                SELECT * FROM content WHERE url = ?
            ''', (url,))

            columns = [description[0] for description in self.cursor.description]
            row = self.cursor.fetchone()

            if row:
                data = dict(zip(columns, row))
                self.close()
                return data

            self.close()
            return None
        except Exception as e:
            print(f"Error fetching data for URL {url}: {e}")
            if hasattr(self, 'close'):
                self.close()
            return None


class DateParser:
    MONTHS = {
        'january': 1, 'february': 2, 'march': 3, 'april': 4,
        'may': 5, 'june': 6, 'july': 7, 'august': 8,
        'september': 9, 'october': 10, 'november': 11, 'december': 12
    }

    @staticmethod
    def parse_release_date(date_text):
        """
        Parse dates in format "Released Month YYYY" to "YYYY-MM-DD"
        Example: "Released March 2020" -> "2020-03-01"
        """
        if not date_text:
            return None

        # Clean up the input text
        date_text = date_text.lower().strip()
        if date_text.startswith('released '):
            date_text = date_text[9:].strip()

        # Extract month and year using regex
        pattern = r'([a-zA-Z]+)\s+(\d{4})'
        match = re.search(pattern, date_text)

        if not match:
            return None

        month_str, year_str = match.groups()
        month_str = month_str.lower()

        # Get month number
        if month_str not in DateParser.MONTHS:
            return None

        month_num = DateParser.MONTHS[month_str]

        # Create datetime object
        try:
            date_obj = datetime(int(year_str), month_num, 1)
            return date_obj.strftime('%Y-%m-%d')
        except ValueError:
            return None


class Scraper:
    def __init__(self, base_url):
        self.base_url = base_url
        self.db = Database()
        self.db.create_tables()
        self.all_links = self.db.get_all_urls()  # Load all URLs from database
        self.date_parser = DateParser()

    def get_html(self, url):
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            print(f'Error fetching {url}: {e}')
            return None

    def get_all_links(self, html, only_releases=False):
        try:
            soup = BeautifulSoup(html, 'html.parser')
        except Exception as e:
            print(f"Error parsing HTML: {e}")
            return []
        links = soup.find_all('a')

        allowed_parts = ['/releases/', '/ranges/', '/hubs/'] if not only_releases else ['/releases/', '/ranges/']
        disallowed_parts = ['facebook.com', 'twitter', 'youtube', '/basket/', '/pages/v/']
        must_contain = ['bigfinish.com']
        parsed_links = []

        for link in links:
            href = link.get('href')
            if href and any(part in href for part in allowed_parts) and not any(
                    part in href for part in disallowed_parts):
                full_url = href if href.startswith('http') else self.base_url + href
                if all(part in full_url for part in must_contain):
                    if full_url not in self.all_links:
                        self.all_links[full_url] = False
                        self.db.add_url(full_url)  # Add new URL to database
                    parsed_links.append(full_url)

        return parsed_links

    def clean_title(self, title):
        if title:
            # Match pattern: <1-6 chars><dot><space><rest>
            match = re.match(r'^([^\s]{1,6})\.\s+(.+)$', title)
            if match:
                prefix = match.group(1)  # The prefix before the dot
                rest = match.group(2)  # Everything after dot and space
                return prefix, rest
        return None, title

    def parse_data(self, url, html):
        try:
            soup = BeautifulSoup(html, 'html.parser')
        except Exception as e:
            print(f"Error parsing {url}: {e}")
            return None
        data = {
            'url': url,
            'title': None,
            'series': None,
            'release_date': None,
            'about': None,
            'background': None,
            'production': None,
            'duration': None,
            'isbn': None,
            'written_by': None,
            'narrated_by': None,
            'cover_url': None,
            'series_tag': None
        }

        product_desc = soup.find('div', {'class': 'product-desc'})
        if product_desc:
            data['title'] = product_desc.find('h3').text.strip() if product_desc.find('h3') else None
            prefix, rest = self.clean_title(data['title'])
            if prefix:
                data['series_tag'] = prefix
                data['title'] = rest

                prefix, rest = self.clean_title(data['title'])
                if prefix:
                    data['series_tag'] = data['series_tag'] + '.' + prefix
                    data['title'] = rest

            data['series'] = product_desc.find('h6').text.strip() if product_desc.find('h6') else None

            if re.search(f"{data['series']}:\\s", data['title'], re.IGNORECASE):
                data['title'] = re.sub(f"{data['series']}:\\s", '', data['title'], flags=re.IGNORECASE)
            # Try replacing " -" with :
            tmp_series = data['series'].replace(" -", ":")
            if re.search(f"{tmp_series}:\\s", data['title'], re.IGNORECASE):
                data['title'] = re.sub(f"{tmp_series}:\\s", '', data['title'], flags=re.IGNORECASE)

        cover_div = soup.find('div', {'class': 'detail-page-image'})
        if cover_div:
            cover_img = cover_div.find('img')
            if cover_img:
                data['cover_url'] = cover_img.get('src')
                if data['cover_url'] and not data['cover_url'].startswith('http'):
                    data['cover_url'] = self.base_url + data['cover_url']
                data['title'] = cover_img.get('alt') if cover_img.get('alt') else data['title']

        # Parse release date
        release_date_div = soup.find('div', {'class': 'release-date'})
        if release_date_div:
            date_text = release_date_div.text.strip()
            parsed_date = DateParser.parse_release_date(date_text)
            if parsed_date:
                data['release_date'] = parsed_date

        # Parse writers and narrators
        paragraphs = product_desc.find_all('p') if product_desc else []
        if len(paragraphs) > 0:
            data['written_by'] = ', '.join([a.text.strip() for a in paragraphs[0].find_all('a')])
        if len(paragraphs) > 1:
            data['narrated_by'] = ', '.join([a.text.strip() for a in paragraphs[1].find_all('a')])

        # Parse tabs content
        for tab_id in ['tab1', 'tab2', 'tab5', 'tab6']:
            tab = soup.find('div', {'id': tab_id})
            if tab:
                content = tab.text.strip()
                if tab_id == 'tab1':
                    data['about'] = content
                elif tab_id == 'tab2':
                    data['background'] = content
                elif tab_id == 'tab5':
                    data['narrated_by'] = ', '.join([a.text.strip() for a in tab.find_all('a')])
                elif tab_id == 'tab6':
                    data['production'] = content
                    # Extract duration and ISBN
                    if 'Duration:' in content:
                        data['duration'] = content.split('Duration: ')[1].split(' ')[0].split('\n')[0]
                    if 'Digital Retail ISBN: ' in content:
                        data['isbn'] = content.split('Digital Retail ISBN: ')[1].split(' ')[0].split('\n')[0]
                        # Check if ISBN is valid
                        if not re.match(r'\d{3}-\d{1,5}-\d{1,7}-\d{1}', data['isbn']):
                            data['isbn'] = None
                    elif 'Physical Retail ISBN: ' in content:
                        data['isbn'] = content.split('Physical Retail ISBN: ')[1].split(' ')[0].split('\n')[0]
                        # Check if ISBN is valid
                        if not re.match(r'\d{3}-\d{1,5}-\d{1,7}-\d{1}', data['isbn']):
                            data['isbn'] = None

        self.db.save_content(data)
        return data

    def run(self):
        if not self.all_links:
            print("No stored URLs found. Starting fresh crawl...")
            html = self.get_html(self.base_url)
            if html:
                self.get_all_links(html)
        else:
            print(f"Loaded {len(self.all_links)} URLs from database")

        while True:
            unvisited_links = [link for link, visited in self.all_links.items()
                               if not visited
                               and "/releases/v/" in link
                               ]

            if not unvisited_links:
                print("No more unvisited links to process")
                break

            print(f"Processing {len(unvisited_links)} unvisited links...")
            for link in unvisited_links:
                print(f'Visiting {link}')
                content = self.get_html(link)
                if content:
                    self.get_all_links(content, True)
                    if "/releases/v/" in link:
                        self.parse_data(link, content)
                    self.db.mark_url_visited(link)
                    self.all_links[link] = True

    def get_statistics(self):
        total_urls = len(self.all_links)
        visited_urls = sum(1 for visited in self.all_links.values() if visited)
        unvisited_urls = total_urls - visited_urls

        print("\nCrawler Statistics:")
        print(f"Total URLs indexed: {total_urls}")
        print(f"Visited URLs: {visited_urls}")
        print(f"Remaining URLs: {unvisited_urls}")


class Search:
    def __init__(self):
        self.base_url = 'https://www.bigfinish.com'

    def search(self, query):

        query = query.replace(':', ' ')
        query = quote(query)

        print(f'https://www.bigfinish.com/search_results/suggest/{query}')

        response = requests.get(f'https://www.bigfinish.com/search_results/suggest/{query}', headers=headers)

        response.raise_for_status()
        results = response.json()

        db = Database()

        # Check if results is a dict and not empty
        if not isinstance(results, dict) or not results:
            return []

        datas = []
        for (index, result) in results.items():
            new_url = f'{self.base_url}/releases/v/{str(result['id'])}'

            # Check if the new url is already in the database

            data = db.return_data_for_url(new_url)
            if data:
                datas.append(data)
                continue

            response = requests.get(new_url, headers=headers)
            response.raise_for_status()
            text = response.text

            data = Scraper(self.base_url).parse_data(new_url, text)
            datas.append(data)

        db.close()

        return datas


if __name__ == '__main__':
    Search().search('Doctor Who')


def test():
    scraper = Scraper('https://www.bigfinish.com')
    try:
        scraper.run()
        scraper.get_statistics()
    except KeyboardInterrupt:
        print("\nCrawling interrupted by user")
        scraper.get_statistics()
    except Exception as e:
        print(f"\nAn error occurred: {e}")
        scraper.get_statistics()
