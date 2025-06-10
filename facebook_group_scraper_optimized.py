import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
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

    def setup_driver(self):
        try:
            print("Configuring Chrome options...")
            options = uc.ChromeOptions()
            
            # GPU Acceleration
            options.add_argument("--enable-gpu")
            options.add_argument("--enable-gpu-rasterization")
            options.add_argument("--enable-zero-copy")
            options.add_argument("--enable-native-gpu-memory-buffers")
            options.add_argument("--ignore-gpu-blocklist")
            options.add_argument("--enable-hardware-overlays")
            
            # Отключаем ненужные функции
            options.add_argument("--disable-software-rasterizer")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-notifications")
            
            # Оптимизация памяти
            options.add_argument("--aggressive-cache-discard")
            options.add_argument("--disable-cache")
            options.add_argument("--disable-application-cache")
            options.add_argument("--disable-offline-load-stale-cache")
            options.add_argument("--disk-cache-size=0")
            
            # Дополнительные оптимизации
            options.add_argument("--disable-features=TranslateUI")
            options.add_argument("--disable-extensions")
            options.add_argument("--window-size=1920,1080")
            
            print("Initializing WebDriver with GPU acceleration...")
            self.driver = uc.Chrome(
                options=options,
                version_main=137
            )
            self.driver.set_window_size(1920, 1080)
            self.driver.set_page_load_timeout(30)
            self.driver.implicitly_wait(1)
            
            # Проверяем статус GPU
            print("\nChecking GPU status...")
            self.driver.get('chrome://gpu')
            time.sleep(1)
            try:
                gpu_info = self.driver.find_element(By.ID, 'basic-info').text
                print(f"GPU Info:\n{gpu_info}")
            except:
                print("Could not get GPU info")
            
            print("WebDriver initialized successfully")
            
        except Exception as e:
            print(f"Error in setup_driver: {str(e)}")
            raise

    @lru_cache(maxsize=128)
    def _get_selector_result(self, selector):
        try:
            return self.driver.find_element(By.CSS_SELECTOR, selector)
        except:
            return None

    def _wait_for_element(self, by, selector, timeout=5):
        try:
            element = WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((by, selector))
            )
            return element
        except TimeoutException:
            return None

    def _find_post_content(self, post):
        """Оптимизированный поиск контента"""
        try:
            elements = post.find_elements(By.CSS_SELECTOR, 
                'div[data-ad-comet-preview="message"], ' +
                'div[data-ad-preview="message"], ' +
                'div.x1iorvi4.x1pi30zi, ' +
                'div[role="article"] div[dir="auto"]'
            )
            
            for element in elements:
                if element.text.strip():
                    return element.text
        except:
            pass
        
        return None

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

    def _scroll_and_extract_posts(self, max_posts):
        posts_data = []
        scroll_attempts = 0
        max_scroll_attempts = 400
        last_height = 0
        no_change_count = 0
        batch_size = 10
        
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
                                save_progress(posts_data, prefix="fb_group_posts_interim")
                
                # Быстрая прокрутка
                self.driver.execute_script("""
                    window.scrollTo(0, document.body.scrollHeight);
                    window.scrollTo(0, document.body.scrollHeight - 100);
                """)
                time.sleep(1)
                
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

    # ... (остальные методы остаются без изменений)

def main():
    GROUP_URL = "https://www.facebook.com/groups/csgotradebuys3ll"
    COOKIES_FILE = "cookies.json"
    MAX_POSTS = 1000
    
    monitor = ResourceMonitor()
    
    print(f"\nStarting Facebook Group Scraper")
    print(f"Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"User: Savka322")
    print(f"Target: {MAX_POSTS} posts")
    
    print("\nInitial resource usage:")
    monitor.print_stats()
    
    scraper = FacebookGroupScraper(cookies_file=COOKIES_FILE)
    
    try:
        posts = scraper.scrape_group(GROUP_URL, max_posts=MAX_POSTS)
        
        if posts:
            output_file = save_progress(posts)
            
            print(f"\nSuccess! Scraped {len(posts)} posts")
            print(f"Results saved to: {output_file}")
            
            print("\nScraping Statistics:")
            print(f"Total posts: {len(posts)}")
            print(f"Posts with images: {len([p for p in posts if p.get('images')])}")
            print(f"Posts with external links: {len([p for p in posts if p.get('external_links')])}")
            
            dates = [p['posted_time'] for p in posts if p.get('posted_time')]
            if dates:
                print(f"Earliest post date: {min(dates)}")
                print(f"Latest post date: {max(dates)}")
            
            print(f"Unique authors: {len(set(p['author']['name'] for p in posts if p.get('author')))}")
            
            print("\nFinal resource usage:")
            monitor.print_stats()
        else:
            print("\nNo posts were scraped!")
            
    except Exception as e:
        print(f"\nError during scraping: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()