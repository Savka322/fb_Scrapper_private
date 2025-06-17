import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from concurrent.futures import ThreadPoolExecutor
from functools import partial, lru_cache
from cachetools import TTLCache
import multiprocessing
import psutil
import GPUtil
import time
import json
from datetime import datetime
import os
import sys
import logging

# Базовая настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ResourceMonitor:
    def __init__(self):
        self.start_time = time.time()
        self.start_cpu = psutil.cpu_percent()
        self.start_memory = psutil.virtual_memory().percent
    
    def print_stats(self):
        elapsed = time.time() - self.start_time
        current_cpu = psutil.cpu_percent()
        current_memory = psutil.virtual_memory().percent
        
        print(f"\nPerformance Statistics:")
        print(f"Time elapsed: {elapsed:.2f} seconds")
        print(f"CPU Usage: {current_cpu}% (change: {current_cpu - self.start_cpu:+.1f}%)")
        print(f"Memory Usage: {current_memory}% (change: {current_memory - self.start_memory:+.1f}%)")
        
        try:
            gpus = GPUtil.getGPUs()
            for gpu in gpus:
                print(f"\nGPU {gpu.id} - {gpu.name}:")
                print(f"Load: {gpu.load*100:.1f}%")
                print(f"Memory: {gpu.memoryUsed}/{gpu.memoryTotal} MB")
                print(f"Temperature: {gpu.temperature}°C")
        except:
            print("No GPU information available")

class FacebookGroupScraper:
    def __init__(self, cookies_file="cookies.json"):
        self.cookies_file = cookies_file
        self.driver = None
        self.url_cache = TTLCache(maxsize=1000, ttl=3600)
        self.processed_urls = set()
        
    def scroll_and_extract_posts(self, max_posts):
        posts_data = []
        scroll_attempts = 0
        max_scroll_attempts = 400
        last_height = 0
        no_change_count = 0
        batch_size = 10
    def _get_post_url(self, post):
        try:
            time_link = post.find_element(By.CSS_SELECTOR, 'a[href*="/posts/"], a[href*="/permalink/"]')
            return time_link.get_attribute('href').split('?')[0]
        except:
            try:
                post_link = post.find_element(By.CSS_SELECTOR, 'a[href*="/groups/"][href*="/post/"]')
                return post_link.get_attribute('href').split('?')[0]
            except:
                return None
        while len(posts_data) < max_posts and scroll_attempts < max_scroll_attempts:
            try:
                posts = self.driver.find_elements(By.CSS_SELECTOR, 'div[role="article"]')
                
                # Обработка постов пакетами
                if len(posts) > 0:
                    new_posts = posts[len(posts_data):]
                    if new_posts:
                        batches = [new_posts[i:i + batch_size] for i in range(0, len(new_posts), batch_size)]
                        for batch in batches:
                            batch_results = self._process_post_batch(batch)
                            posts_data.extend(batch_results)
                            
                            if len(posts_data) >= max_posts:
                                break
                            
                            if len(posts_data) % 10 == 0:
                                print(f"Extracted post {len(posts_data)}/{max_posts} ({(len(posts_data)/max_posts*100):.1f}%)")
                                self.save_to_file(posts_data, prefix="fb_group_posts_interim")
                
                # Быстрая прокрутка
                self.driver.execute_script("""
                    window.scrollTo(0, document.body.scrollHeight);
                    window.scrollTo(0, document.body.scrollHeight - 100);
                """)
                time.sleep(1)
                
                # Проверка прокрутки
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    no_change_count += 1
                    if no_change_count >= 5:
                        self.driver.execute_script("""
                            window.scrollTo(0, document.body.scrollHeight/2);
                            setTimeout(() => {
                                window.scrollTo(0, document.body.scrollHeight);
                            }, 500);
                        """)
                        time.sleep(2)
                        no_change_count = 0
                else:
                    no_change_count = 0
                
                last_height = new_height
                scroll_attempts += 1
                
            except Exception as e:
                print(f"Error during scrolling: {e}")
                scroll_attempts += 1
                time.sleep(1)
        
        return posts_data

    def _process_post_batch(self, posts):
        """Параллельная обработка пачки постов"""
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for post in posts:
                post_url = self._get_post_url(post)
                if post_url and post_url not in self.processed_urls:
                    self.processed_urls.add(post_url)
                    futures.append(executor.submit(self._extract_post_data, post))
            
            results = []
            for future in futures:
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                except Exception as e:
                    print(f"Error processing post: {e}")
            
            return results

    def _extract_post_data(self, post):
        try:
            content = self._find_post_content(post)
            if not content:
                return None
                
            posted_time = self._find_post_time(post)
            author_info = self._find_post_author(post)
            post_url = self._get_post_url(post)
            links = self._find_post_links(post)
            images = self._find_post_images(post)
            
            if not (content and posted_time and author_info and post_url):
                return None
                
            return {
                'author': author_info,
                'content': content,
                'posted_time': posted_time,
                'post_url': post_url,
                'external_links': links,
                'images': images,
                'scraped_time': '2025-06-17 12:20:06',  # Обновленная дата
                'scraper_user': 'Savka322'  # Ваш логин
            }
            
        except Exception as e:
            print(f"Error in _extract_post_data: {e}")
            return None

    def _find_post_time(self, post):
        time_selectors = [
            'a[role="link"] > span',
            'span.x4k7w5x a span',
            'a.x1i10hfl span',
            'span.x1xp8e9x a',
            'a[role="link"][tabindex="0"] span'
        ]
        
        for selector in time_selectors:
            try:
                element = post.find_element(By.CSS_SELECTOR, selector)
                time_text = element.get_attribute('title') or element.text
                if time_text:
                    return time_text
            except:
                continue
                
        return None

    def _find_post_author(self, post):
        author_selectors = [
            'h2 a',
            'span.x193iq5w a',
            'h2.x1heor9g a',
            'div.x1heor9g a[role="link"]',
            'a.x1i10hfl[role="link"]:not([href*="groups"])'
        ]
        
        for selector in author_selectors:
            try:
                element = post.find_element(By.CSS_SELECTOR, selector)
                return {
                    'name': element.text,
                    'profile_url': element.get_attribute('href')
                }
            except:
                continue
                
        return None

    def _find_post_links(self, post):
        link_selectors = [
            'a[href*="http"]:not([href*="facebook.com"])',
            'a[href*="steam"]',
            'a[href*="tradeoffer"]',
            'div[data-ad-comet-preview="message"] a'
        ]
        
        links = []
        for selector in link_selectors:
            try:
                elements = post.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    href = element.get_attribute('href')
                    if href and not href.startswith('https://www.facebook.com/'):
                        clean_link = href.split('?fbclid=')[0]
                        if clean_link not in links:
                            links.append(clean_link)
            except:
                continue
                
        return links

    def _find_post_images(self, post):
        image_selectors = [
            'img[src*="scontent"]',
            'img[data-visualcompletion="media-vc-image"]',
            'a[href*="photo.php"] img',
            'div[role="article"] img:not([src*="emoji"])'
        ]
        
        images = []
        for selector in image_selectors:
            try:
                elements = post.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    src = element.get_attribute('src')
                    if src and 'emoji' not in src and src not in images:
                        images.append(src)
            except:
                continue
                
        return images

    def save_to_file(self, posts, filename_prefix="fb_group_posts"):
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        output_file = f"{filename_prefix}_{timestamp}.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(posts, f, ensure_ascii=False, indent=2)
        
        return output_file    
    def setup_driver(self):
        try:
            print("Configuring Chrome options...")
            options = uc.ChromeOptions()
            
            # Основные опции
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-notifications")
            options.add_argument("--window-size=1920,1080")
            
            # Отключение кэша
            options.add_argument("--aggressive-cache-discard")
            options.add_argument("--disable-cache")
            options.add_argument("--disable-application-cache")
            options.add_argument("--disable-offline-load-stale-cache")
            options.add_argument("--disk-cache-size=0")
            
            print("Initializing WebDriver...")
            self.driver = uc.Chrome(
                options=options,
                version_main=None  # Автоопределение версии
            )
            self.driver.set_window_size(1920, 1080)
            self.driver.set_page_load_timeout(30)
            self.driver.implicitly_wait(10)
            
            print("WebDriver initialized successfully")
            
        except Exception as e:
            print(f"Error in setup_driver: {str(e)}")
            raise

    def manual_login(self, url):
        try:
            print(f"\nNavigating to {url}")
            self.driver.get(url)
            
            print("\n=== MANUAL LOGIN REQUIRED ===")
            print("1. Please log in manually in the browser window")
            print("2. After successful login, press Enter in this console")
            print("===============================\n")
            
            input("Press Enter after logging in...")
            time.sleep(5)  # Даем время для завершения логина
            
            # Сохраняем cookies после успешного входа
            cookies = self.driver.get_cookies()
            with open('facebook_cookies.json', 'w') as f:
                json.dump(cookies, f)
            print("Cookies saved successfully")
            
        except Exception as e:
            print(f"Error during manual login: {str(e)}")
            raise

    def load_cookies(self):
        try:
            if not os.path.exists(self.cookies_file):
                raise FileNotFoundError(f"Cookie file {self.cookies_file} not found!")
            
            print("Reading cookies file...")
            with open(self.cookies_file, 'r') as f:
                cookies = json.load(f)
            
            print("Loading cookies into browser...")
            self.driver.get("https://www.facebook.com")
            time.sleep(3)
            
            for cookie in cookies:
                try:
                    cookie['secure'] = True
                    if 'expiry' in cookie:
                        del cookie['expiry']
                    self.driver.add_cookie(cookie)
                except Exception as e:
                    print(f"Error adding cookie {cookie.get('name')}: {e}")
            
            print("Cookies loaded successfully")
            self.driver.refresh()
            time.sleep(3)
            
        except Exception as e:
            print(f"Error in load_cookies: {str(e)}")
            raise

    # ... [остальные методы остаются без изменений из вашего старого кода] ...

def main():
    GROUP_URL = "https://www.facebook.com/groups/BHPHSuccess"
    COOKIES_FILE = "cookies.json"
    MAX_POSTS = 5

    monitor = ResourceMonitor()
    print("\nStarting Facebook Group Scraper")
    print(f"Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Target: {MAX_POSTS} posts")

    try:
        scraper = FacebookGroupScraper(cookies_file=COOKIES_FILE)
        scraper.setup_driver()
        
        # Пробуем сначала использовать сохраненные cookies
        try:
            scraper.load_cookies()
            scraper.driver.get(GROUP_URL)
            time.sleep(5)
            
            # Проверяем, нужен ли ручной логин
            if "/login" in scraper.driver.current_url:
                print("Cookies expired or invalid, switching to manual login...")
                scraper.manual_login(GROUP_URL)
        except Exception as e:
            print(f"Error with cookies, switching to manual login: {e}")
            scraper.manual_login(GROUP_URL)

        print("\nStarting to scrape posts...")
        posts = scraper.scroll_and_extract_posts(MAX_POSTS)

        if posts:
            output_file = scraper.save_to_file(posts)
            print(f"\nSuccess! Scraped {len(posts)} posts")
            print(f"Results saved to: {output_file}")
        else:
            print("\nNo posts were scraped!")

    except Exception as e:
        print(f"\nError during scraping: {e}")
        sys.exit(1)
    finally:
        if scraper and scraper.driver:
            scraper.driver.quit()
        print("\nFinal resource usage:")
        monitor.print_stats()

if __name__ == "__main__":
    main()
