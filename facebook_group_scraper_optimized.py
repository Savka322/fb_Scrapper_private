import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from functools import lru_cache
from cachetools import TTLCache
import undetected_chromedriver as uc
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
        self.profile_cache = {}

    def setup_driver(self):
        try:
            print("Configuring Chrome options...")
            options = uc.ChromeOptions()
            options = uc.ChromeOptions()
            options.add_argument("--user-data-dir=/path/to/my_chrome_profile")
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
            self.driver.implicitly_wait(2)

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
        """
        Click 'See more'/'ещё' in posts and comments, if present.
        """
        try:
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

    def _find_post_datetime(self, post):
        """
        Try to extract the post's published datetime as string from <abbr> or <span> or <a> (has aria-label or title with date).
        """
        try:
            # Most Facebook posts have a date in <a> with aria-label or title
            time_elems = post.find_elements(By.XPATH,
                ".//a[contains(@href,'/posts/') or contains(@href,'permalink') or contains(@aria-label,'·') or @aria-label or @title or .//abbr]"
            )
            for elem in time_elems:
                # aria-label usually has readable date
                date_str = elem.get_attribute("aria-label") or elem.get_attribute("title") or elem.text
                if date_str and any(char.isdigit() for char in date_str):
                    return date_str.strip()
            # Try abbr
            abbrs = post.find_elements(By.TAG_NAME, "abbr")
            for ab in abbrs:
                date_str = ab.get_attribute("aria-label") or ab.get_attribute("title") or ab.text
                if date_str and any(char.isdigit() for char in date_str):
                    return date_str.strip()
        except Exception:
            pass
        return None

    def _find_author(self, post):
        """
        Find author name and profile url for post or comment.
        """
        try:
            # Facebook post author is usually the first <a> in the header, not containing group links, not 'Show translation'
            author_link = None
            possible_links = post.find_elements(
                By.XPATH,
                ".//a[not(contains(@href, '/groups/')) and not(contains(@href,'/hashtag/')) and not(contains(text(),'Показать перевод')) and not(contains(text(),'Show translation')) and (contains(@href,'facebook.com/') or contains(@href,'profile.php') or contains(@href,'/people/'))]"
            )
            for a in possible_links:
                n = a.text.strip()
                href = a.get_attribute("href") or ""
                if n and len(n) < 60 and "facebook.com" in href:
                    author_link = a
                    break
            if author_link:
                return {
                    "name": author_link.text.strip(),
                    "profile_url": author_link.get_attribute("href")
                }
        except Exception:
            pass
        return {"name": None, "profile_url": None}

    def _collect_profile_info(self, profile_url):
        """
        Collects public info (city, about etc.) from user's profile.
        Caches result to avoid re-parsing same profiles.
        """
        if not profile_url:
            return {}
        if profile_url in self.profile_cache:
            return self.profile_cache[profile_url]
        info = {}
        try:
            self.driver.execute_script("window.open('');")
            self.driver.switch_to.window(self.driver.window_handles[-1])
            self.driver.get(profile_url)
            time.sleep(2)
            # About or Intro block: city, work, education, about etc.
            try:
                intro = self.driver.find_elements(By.XPATH, "//div[contains(@data-pagelet,'ProfileTilesFeed_0') or contains(@data-pagelet,'ProfileTimeline')]")
                if not intro:
                    intro = self.driver.find_elements(By.XPATH, "//div[contains(@data-pagelet,'ProfileTimeline')]")
                text_blocks = []
                for block in intro:
                    for div in block.find_elements(By.XPATH, ".//div[.//span]"):
                        txt = div.text.strip()
                        if txt:
                            text_blocks.append(txt)
                # Simple heuristics: save all blocks with ':' or enough length
                info['public_blocks'] = [x for x in text_blocks if len(x) > 3]
            except Exception:
                pass
            # Try to get user id from url (for /profile.php?id= or /people/...)
            if "profile.php?id=" in profile_url:
                info['user_id'] = profile_url.split("profile.php?id=")[-1].split("&")[0]
            elif "/people/" in profile_url:
                parts = profile_url.split("/people/")
                if len(parts) > 1:
                    info['user_id'] = parts[1].split("/")[1] if len(parts[1].split("/")) > 1 else None
            # About info
            about_links = self.driver.find_elements(By.XPATH, "//a[contains(@href,'about')]")
            if about_links:
                about_links[0].click()
                time.sleep(1.5)
                # Try intro/about section again
                about_blocks = self.driver.find_elements(By.XPATH, "//div[contains(@data-pagelet,'ProfileTilesFeed_0')]")
                about_texts = []
                for block in about_blocks:
                    for div in block.find_elements(By.XPATH, ".//div[.//span]"):
                        txt = div.text.strip()
                        if txt:
                            about_texts.append(txt)
                if about_texts:
                    info['about_blocks'] = [x for x in about_texts if len(x) > 3]
        except Exception as e:
            info['_profile_error'] = str(e)
        finally:
           # self.driver.close()
            self.driver.switch_to.window(self.driver.window_handles[0])
        self.profile_cache[profile_url] = info
        return info

    def _collect_comments(self, post):
        """
        Recursively collect all comments and their replies for a post.
        """
        def expand_all_comments(post_elem):
            expanded = False
            # Expand 'View more comments', 'See more comments', 'Показать ещё комментарии', 'See more replies'
            btn_texts = [
                'Показать ещё', 'See more', 'View more', 'Показать ещё комментарии', 'View more comments', 'See previous', 'See more replies', 'Показать больше ответов'
            ]
            try:
                for btn_text in btn_texts:
                    buttons = post_elem.find_elements(By.XPATH, ".//span[contains(text(), '{}')]".format(btn_text))
                    for btn in buttons:
                        try:
                            self.driver.execute_script("arguments[0].scrollIntoView(true);", btn)
                            btn.click()
                            expanded = True
                            time.sleep(0.5)
                        except Exception:
                            pass
            except Exception:
                pass
            return expanded

        # Keep expanding comments until nothing else is found
        while expand_all_comments(post):
            time.sleep(0.6)

        # Now parse the visible comments tree
        def parse_comment(comment_elem):
            try:
                # Expand "ещё"/"See more" for long comments
                self.expand_see_more(comment_elem)
                # Get comment text
                comment_text = ""
                content_blocks = comment_elem.find_elements(By.CSS_SELECTOR, 'div[dir="auto"]')
                for cb in content_blocks:
                    txt = cb.text.strip()
                    if txt and len(txt) > len(comment_text):
                        comment_text = txt
                # Get published time
                published_time = None
                time_elems = comment_elem.find_elements(By.XPATH, ".//a[@aria-label or @title]")
                for te in time_elems:
                    date_str = te.get_attribute("aria-label") or te.get_attribute("title") or te.text
                    if date_str and any(char.isdigit() for char in date_str):
                        published_time = date_str.strip()
                        break
                # Get author
                author = self._find_author(comment_elem)
                # Get author profile info (cached)
                author['profile_info'] = self._collect_profile_info(author['profile_url'])
                # Recursively get replies
                replies = []
                reply_blocks = comment_elem.find_elements(By.XPATH, ".//ul")
                for ul in reply_blocks:
                    for li in ul.find_elements(By.XPATH, "./li"):
                        reply = parse_comment(li)
                        if reply:
                            replies.append(reply)
                return {
                    "text": comment_text,
                    "published_time": published_time,
                    "author": author,
                    "replies": replies
                }
            except Exception as e:
                return None

        comments = []
        try:
            # Top-level comments: Facebook uses <ul> or <div> blocks for comments
            comment_blocks = post.find_elements(By.XPATH, ".//ul[contains(@aria-label,'Комментарии') or contains(@aria-label,'Comments')]/li | .//div[contains(@aria-label,'Комментарии') or contains(@aria-label,'Comments')]/div")
            for cb in comment_blocks:
                comment = parse_comment(cb)
                if comment:
                    comments.append(comment)
        except Exception:
            pass
        return comments

    def _process_post(self, post):
        try:
            self.expand_see_more(post)
            post_url = self._get_post_url(post)
            if not post_url or post_url in self.processed_urls:
                return None
            content = self._find_post_content(post)
            if not content:
                return None

            # Author
            author = self._find_author(post)
            author['profile_info'] = self._collect_profile_info(author['profile_url'])

            # Published time
            published_time = self._find_post_datetime(post)

            # Comments (with recursion)
            comments = self._collect_comments(post)

            return {
                "url": post_url,
                "content": content,
                "published_time": published_time,
                "author": author,
                "comments": comments
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
          # После успешного логина сохраняем куки
        cookies = self.driver.get_cookies()
        with open(self.cookies_file, "w", encoding="utf-8") as f:
            json.dump(cookies, f)
        print("Куки сохранены!")

def main():
    GROUP_URL = "https://www.facebook.com/groups/BHPHSuccess"
    COOKIES_FILE = "cookies.json"
    MAX_POSTS = 10

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
