import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from concurrent.futures import ThreadPoolExecutor
from functools import partial, lru_cache
from cachetools import TTLCache
import multiprocessing
import psutil
import GPUtil
import time
import json
import random
from datetime import datetime
import os
import sys
import logging
import traceback

# Настройка логирования с выводом в консоль и файл
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fb_scraper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class ResourceMonitor:
    def __init__(self):
        self.start_time = time.time()
        self.start_cpu = psutil.cpu_percent()
        self.start_memory = psutil.virtual_memory().percent

    def print_stats(self):
        try:
            elapsed = time.time() - self.start_time
            current_cpu = psutil.cpu_percent()
            current_memory = psutil.virtual_memory().percent

            logger.info("\nPerformance Statistics:")
            logger.info(f"Time elapsed: {elapsed:.2f} seconds")
            logger.info(f"CPU Usage: {current_cpu}% (change: {current_cpu - self.start_cpu:+.1f}%)")
            logger.info(f"Memory Usage: {current_memory}% (change: {current_memory - self.start_memory:+.1f}%)")

            try:
                gpus = GPUtil.getGPUs()
                for gpu in gpus:
                    logger.info(f"\nGPU {gpu.id} - {gpu.name}:")
                    logger.info(f"Load: {gpu.load*100:.1f}%")
                    logger.info(f"Memory: {gpu.memoryUsed}/{gpu.memoryTotal} MB")
                    logger.info(f"Temperature: {gpu.temperature}°C")
            except Exception as e:
                logger.info(f"No GPU information available: {str(e)}")
        except Exception as e:
            logger.error(f"Error in print_stats: {str(e)}")

class FacebookGroupScraper:
    def __init__(self, cookies_file="cookies.json"):
        self.cookies_file = cookies_file
        self.driver = None
        self.url_cache = TTLCache(maxsize=1000, ttl=3600)
        self.processed_urls = set()
        self.processed_posts = []
        self.max_no_new_posts = 5

    def get_random_user_agent(self):
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0"
        ]
        return random.choice(user_agents)

    def setup_driver(self):
        try:
            logger.info("Starting setup_driver...")
            options = uc.ChromeOptions()
            
            # Добавляем все опции с логированием
            chrome_options = [
                ("user-agent", self.get_random_user_agent()),
                ("--enable-gpu", None),
                ("--no-sandbox", None),
                ("--disable-dev-shm-usage", None),
                ("--disable-notifications", None),
                ("--window-size=1920,1080", None)
            ]

            for opt, value in chrome_options:
                try:
                    if value:
                        options.add_argument(f"{opt}={value}")
                    else:
                        options.add_argument(opt)
                    logger.info(f"Added Chrome option: {opt}")
                except Exception as e:
                    logger.error(f"Error adding Chrome option {opt}: {str(e)}")

            logger.info("Initializing Chrome WebDriver...")
            self.driver = uc.Chrome(options=options)
            
            logger.info("Setting window size...")
            self.driver.set_window_size(1920, 1080)
            
            logger.info("Setting page load timeout...")
            self.driver.set_page_load_timeout(30)
            
            logger.info("Setting implicit wait...")
            self.driver.implicitly_wait(10)  # Увеличили время ожидания

            logger.info("WebDriver setup completed successfully")
            return True
        except Exception as e:
            logger.error(f"Error in setup_driver: {str(e)}")
            logger.error(traceback.format_exc())
            return False

    def manual_login(self, url):
        try:
            logger.info(f"Attempting to navigate to {url}")
            self.driver.get(url)
            logger.info("Page loaded successfully")
            
            logger.info("Waiting for manual login...")
            print("\n=== MANUAL LOGIN REQUIRED ===")
            print("1. Please log in manually in the browser window")
            print("2. After successful login, press Enter in this console")
            print("===============================\n")
            
            input("Press Enter after logging in...")
            
            logger.info("Manual login completed")
            time.sleep(5)  # Даем время для полной загрузки после логина
            return True
        except Exception as e:
            logger.error(f"Error during manual login: {str(e)}")
            logger.error(traceback.format_exc())
            return False

def main():
    try:
        GROUP_URL = "https://www.facebook.com/groups/BHPHSuccess"
        COOKIES_FILE = "cookies.json"
        MAX_POSTS = 5

        logger.info("=== Starting Facebook Group Scraper ===")
        logger.info(f"Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"User: Savka322")
        logger.info(f"Target URL: {GROUP_URL}")
        logger.info(f"Target posts: {MAX_POSTS}")

        monitor = ResourceMonitor()
        logger.info("Resource monitor initialized")

        scraper = FacebookGroupScraper(cookies_file=COOKIES_FILE)
        logger.info("Scraper instance created")

        if not scraper.setup_driver():
            raise Exception("Failed to setup WebDriver")

        if not scraper.manual_login(GROUP_URL):
            raise Exception("Failed to complete manual login")

        logger.info("Starting to scrape posts...")
        # Здесь добавить остальную логику скрапинга...

    except Exception as e:
        logger.error(f"Critical error in main: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        try:
            if scraper and scraper.driver:
                logger.info("Closing WebDriver...")
                scraper.driver.quit()
        except Exception as e:
            logger.error(f"Error while closing driver: {str(e)}")
        
        logger.info("=== Scraper finished ===")

if __name__ == "__main__":
    main()
