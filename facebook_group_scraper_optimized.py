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

class ResourceMonitor:
    def __init__(self):
        self.start_time = time.time()
        self.start_cpu = psutil.cpu_percent()
        self.start_memory = psutil.virtual_memory().percent

    def print_stats(self):
        elapsed = time.time() - self.start_time
        current_cpu = psutil.cpu_percent()
        current_memory = psutil.virtual_memory().percent

        print("\nPerformance Statistics:")
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
        self.processed_posts = []
        self.max_no_new_posts = 5

    def setup_driver(self):
        try:
            print("Configuring Chrome options...")
            options = uc.ChromeOptions()
            options.add_argument("--enable-gpu")
            options.add_argument("--enable-gpu-rasterization")
            options.add_argument("--enable-zero-copy")
            options.add_argument("--enable-native-gpu-memory-buffers")
            options.add_argument("--ignore-gpu-blacklist")
            options.add_argument("--enable-hardware-overlays")
            options.add_argument("--disable-software-rasterizer")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-notifications")
            options.add_argument("--aggressive-cache-discard")
            options.add_argument("--disable-cache")
            options.add_argument("--disable-application-cache")
            options.add_argument("--disable-offline-load-stale-cache")
            options.add_argument("--disk-cache-size=0")
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
        try:
            elements = post.find_elements(By.CSS_SELECTOR,
                'div[data-ad-comet-preview="message"], '
                'div[data-ad-preview="message"], '
                'div.x1iorvi4.x1pi30zi, '
                'div[role="article"] div[dir="auto"]'
            )
            for element in elements:
                if element.text.strip():
                    return element.text
        except:
            pass
        return None

    def _get_post_url(self, post):
        try:
            anchors = post.find_elements(By.TAG_NAME, "a")
            for a in anchors:
                href = a.get_attribute("href")
                if href and "/posts/" in href:
                    return href.split("?")[0]
        except:
            pass
        return None

    def expand_see_more(self, post):
     try:
        # Более универсальный XPATH для кнопки "ещё"
        buttons = post.find_elements(
            By.XPATH,
            ".//*[self::span or self::a or self::div][contains(text(),'ещё') or contains(text(),'See more')]"
        )
        for btn in buttons:
            try:
                self.driver.execute_script("arguments[0].scrollIntoView(true);", btn)
                btn.click()
                time.sleep(0.3)
            except Exception:
                pass
     except Exception:
        pass

    def _process_post(self, post):
     try:
        self.expand_see_more(post)
        post_url = self._get_post_url(post)
        if not post_url or post_url in self.processed_urls:
            return None
        content = self._find_post_content(post)
        if not content:
            return None

        # Новый способ: ищем имя и ссылку на профиль автора
        profile_url = None
        name = None
        try:
            # Обычно имя — это <strong> или <span> внутри <a> в начале поста
            # Также часто рядом есть время — ищем <a> с видимым текстом не служебного типа
            # Универсальный XPATH:
            author_link = None
            # Часто имя автора находится в h2 или div, первый <a>
            possible_links = post.find_elements(
                By.XPATH,
                ".//h2//a[not(contains(@href, '/groups/')) or contains(@href, 'profile.php')] | .//strong//a | .//a[contains(@href,'facebook.com') and not(contains(text(),'Показать перевод')) and not(contains(text(),'Show translation'))]"
            )
            for a in possible_links:
                n = a.text.strip()
                if n and len(n) < 50 and "facebook.com" in (a.get_attribute("href") or "") and (
                    "/profile.php" in (a.get_attribute("href") or "") or "/people/" in (a.get_attribute("href") or "")
                ):
                    author_link = a
                    break
            if author_link is not None:
                profile_url = author_link.get_attribute("href")
                name = author_link.text.strip()
        except Exception:
            pass

        return {
            "url": post_url,
            "content": content,
            "profile_url": profile_url,
            "name": name,
        }
     except Exception as e:
        print(f"Error processing post: {e}")
        return None


    def scroll_and_extract_posts(self, max_posts):
        posts_data = []
        no_new_posts_count = 0

        while len(posts_data) < max_posts and no_new_posts_count < self.max_no_new_posts:
            try:
                posts = self.driver.find_elements(By.CSS_SELECTOR, 'div[role="article"]')
                new_posts_found = 0
                for post in posts:
                    post_url = self._get_post_url(post)
                    if post_url and post_url not in self.processed_urls:
                        result = self._process_post(post)
                        if result:
                            self.processed_urls.add(post_url)
                            posts_data.append(result)
                            new_posts_found += 1
                            print(f"Собрано постов: {len(posts_data)} / {max_posts}")

                if new_posts_found == 0:
                    no_new_posts_count += 1
                else:
                    no_new_posts_count = 0

                self.driver.execute_script(
                    "window.scrollTo(0, document.body.scrollHeight);"
                )
                time.sleep(1)
            except Exception as e:
                print(f"Error during scrolling: {e}")
                time.sleep(1)

        return posts_data

    def save_to_file(self, posts, filename_prefix="fb_group_posts"):
        dt = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        file_path = f"{filename_prefix}_{dt}.json"
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(posts, f, ensure_ascii=False, indent=2)
        print(f"Results saved to: {file_path}")
        return file_path

    def load_cookies(self):
        if not os.path.exists(self.cookies_file):
            raise FileNotFoundError(f"Cookies file '{self.cookies_file}' not found!")
        with open(self.cookies_file, "r", encoding="utf-8") as f:
            cookies = json.load(f)
        return cookies

    def login_with_cookies(self, url):
        self.driver.get(url)
        cookies = self.load_cookies()
        for cookie in cookies:
            self.driver.add_cookie(cookie)
        self.driver.refresh()
        time.sleep(3)

    def manual_login(self, url):
        self.driver.get(url)
        print("Пожалуйста, залогиньтесь вручную в открывшемся окне браузера.")
        input("После успешного входа нажмите Enter здесь в консоли...")

def main():
    GROUP_URL = "https://www.facebook.com/groups/BHPHSuccess"
    COOKIES_FILE = "cookies.json"
    MAX_POSTS = 5

    monitor = ResourceMonitor()

    print("\nStarting Facebook Group Scraper")
    print(f"Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
    print("User: Savka322")
    print(f"Target: {MAX_POSTS} posts")

    print("\nInitial resource usage:")
    monitor.print_stats()

    scraper = FacebookGroupScraper(cookies_file=COOKIES_FILE)
    posts = []
    try:
        scraper.setup_driver()
        scraper.manual_login(GROUP_URL)
        print("\nScraping posts...")
        try:
            posts = scraper.scroll_and_extract_posts(MAX_POSTS)
        except KeyboardInterrupt:
            print("\nОстановка по Ctrl+C! Сохраняем собранные посты...")

        if posts:
            output_file = scraper.save_to_file(posts)
            print(f"\nSuccess! Scraped {len(posts)} posts")
            print(f"Results saved to: {output_file}")

            print("\nScraping Statistics:")
            print(f"Total posts: {len(posts)}")
            print(f"Unique post URLs: {len(set(p['url'] for p in posts))}")
        else:
            print("\nNo posts were scraped!")

        print("\nFinal resource usage:")
        monitor.print_stats()
    except Exception as e:
        print(f"\nError during scraping: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
