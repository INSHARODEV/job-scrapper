import requests
import time
import json
import hashlib
import logging
import random
import time
from datetime import datetime, timedelta
from typing import List, Dict, Set
from dataclasses import dataclass, asdict
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.common.exceptions import NoSuchElementException
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class Job:
    company_name: str
    platform: str
    job_title: str
    job_type: str  
    job_link: str
    posted_time: str
    location: str = ""
    description_snippet: str = ""
    
    def to_dict(self) -> Dict:
        return asdict(self)
    
    def get_hash(self) -> str:
        """Generate unique hash for duplicate detection"""
        unique_string = f"{self.company_name}_{self.job_title}_{self.platform}"
        return hashlib.md5(unique_string.encode()).hexdigest()

class JobScraper:
    def __init__(self, config_file: str = "config.json"):
        self.config = self.load_config(config_file)
        self.driver = None
        self.seen_jobs: Set[str] = set()
        
        # Prioritize environment variables over config file
        self.api_key = os.getenv('AIRTABLE_API_KEY')
        self.base_id = os.getenv('AIRTABLE_BASE_ID') 
        self.table_name = os.getenv('AIRTABLE_TABLE_NAME', 'Jobs')
        self.script_runs_table_id = os.getenv('AIRTABLE_SCRIPT_RUNS_TABLE_ID')
        
        # Only fall back to config if env vars are not set
        if not self.api_key:
            self.api_key = self.config.get('airtable', {}).get('api_key')
        if not self.base_id:
            self.base_id = self.config.get('airtable', {}).get('base_id')
        if not self.script_runs_table_id:
            self.script_runs_table_id = self.config.get('airtable', {}).get('script_runs_table_id')
        
        # Validate required credentials
        if not all([self.api_key, self.base_id]):
            raise ValueError("Missing required Airtable credentials. Please set AIRTABLE_API_KEY and AIRTABLE_BASE_ID environment variables")
        
        self.api_url = f"https://api.airtable.com/v0/{self.base_id}/{self.table_name}"
        
        self.filtered_companies = {
            'large_companies': [
                'google', 'microsoft', 'amazon', 'apple', 'meta', 'netflix', 'tesla',
                'saudi aramco', 'sabic', 'stc', 'mobily', 'zain', 'accenture',
                'deloitte', 'pwc', 'kpmg', 'ey', 'ibm', 'oracle', 'sap'
            ],
            'hr_firms': [
                'randstad', 'manpower', 'adecco', 'hays', 'robert half',
                'recruitment', 'talent', 'staffing', 'hr solutions', 'workforce'
            ],
            'government': [
                'ministry', 'government', 'municipal', 'authority', 'commission',
                'council', 'public sector', 'gov.sa', 'moe', 'moh', 'mci'
            ]
        }
        self.target_roles = [
            'graphic designer', 'full stack developer','ui-ux designer', 
            'motion graphic designer', 'frontend developer', 'backend developer',
            'web developer', 'mobile developer', 'react developer', 'angular developer',"مصمم جرافيك"
        ]
   
    def load_config(self, config_file: str) -> Dict:
        """Load configuration from JSON file"""
        try:
            with open(config_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            # Only create template for local development, not in GitHub Actions
            if not os.getenv('GITHUB_ACTIONS'):
                logger.error(f"Config file {config_file} not found. Creating template...")
                self.create_config_template(config_file)
            else:
                logger.error(f"Config file {config_file} not found in GitHub Actions!")
            return {}
        
    def create_config_template(self, config_file: str):
        """Create a template configuration file"""
        template = {
            "google_sheets": {
                "credentials_file": "credentials.json",
                "spreadsheet_name": "Saudi Arabia Jobs",
                "worksheet_name": "Jobs"
            },
            "airtable": {
                "api_key": "your_airtable_api_key_here",
                "base_id": "your_airtable_base_id_here",
                "table_name": "Jobs",
                "script_runs_table_id": "your_script_runs_table_id_here"
            },
            "slack": {
                "webhook_url": "your_slack_webhook_url"
            },
            "scraping": {
                "headless": True,
                "delay_between_requests": 1,
                "max_pages_per_site": 20
            },
        }
        with open(config_file, 'w') as f:
            json.dump(template, f, indent=2)
        logger.info(f"Created template config file: {config_file}")

    def setup_driver(self):
        """Setup Chrome driver with enhanced anti-detection and user agent rotation"""
        
        # Updated user agents with more recent versions
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        ]
        
        # Select random user agent
        selected_user_agent = random.choice(user_agents)
        logger.info(f"Using user agent: {selected_user_agent}")
        
        # Configure Chrome options
        chrome_options = Options()
        # chrome_options.add_argument('--headless') 
        # Basic configuration
        if self.config.get('scraping', {}).get('headless', True):
            chrome_options.add_argument("--headless")
            logger.info("Running in headless mode")
        
        # Security and performance options
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-images")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        # chrome_options.add_argument("--disable-javascript")  # Optional: disable JS if not needed
        
        # Anti-detection measures
        chrome_options.add_argument("--disable-ipc-flooding-protection")
        
        # User agent and window settings
        chrome_options.add_argument(f"--user-agent={selected_user_agent}")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--start-maximized")
        
        # Memory and performance optimizations
        chrome_options.add_argument("--memory-pressure-off")
        chrome_options.add_argument("--max_old_space_size=4096")
        chrome_options.add_argument("--disable-background-timer-throttling")
        chrome_options.add_argument("--disable-backgrounding-occluded-windows")
        chrome_options.add_argument("--disable-renderer-backgrounding")
        
        # Network and cache settings
        chrome_options.add_argument("--aggressive-cache-discard")
        chrome_options.add_argument("--disable-background-networking")
        
        # Additional prefs for better performance
        prefs = {
            "profile.default_content_setting_values": {
                "notifications": 2,  # Block notifications
                "images": 2,  # Block images for faster loading
                "plugins": 2,  # Block plugins
                "popups": 2,  # Block popups
                "geolocation": 2,  # Block location requests
                "media_stream": 2,  # Block media stream requests
            },
            "profile.managed_default_content_settings": {
                "images": 2
            }
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        try:
            # Initialize driver
            logger.info("Initializing Chrome WebDriver...")
            self.driver = webdriver.Chrome(options=chrome_options)
            
            # Set timeouts
            self.driver.implicitly_wait(10)
            self.driver.set_page_load_timeout(30)
            
            # Execute anti-detection scripts
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            # Additional anti-detection measures
            self.driver.execute_script("""
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5]
                });
            """)
            
            self.driver.execute_script("""
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en']
                });
            """)
            
            self.driver.execute_script("""
                const getParameter = WebGLRenderingContext.getParameter;
                WebGLRenderingContext.prototype.getParameter = function(parameter) {
                    if (parameter === 37445) {
                        return 'Intel Inc.';
                    }
                    if (parameter === 37446) {
                        return 'Intel Iris OpenGL Engine';
                    }
                    return getParameter(parameter);
                };
            """)
            
            logger.info("Chrome WebDriver initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Chrome WebDriver: {e}")
            raise
        
        return self.driver
   
    def is_company_filtered(self, company_name: str) -> bool:
        """Check if company should be filtered out"""
        company_lower = company_name.lower()
        
        for category, companies in self.filtered_companies.items():
            for filtered_company in companies:
                if filtered_company in company_lower:
                    logger.info(f"Filtering out {company_name} - matches {category}")
                    return True
        return False
    
    def is_relevant_role(self, job_title: str) -> bool:
        """Check if job title matches target roles"""
        title_lower = job_title.lower()
        return any(role in title_lower for role in self.target_roles)
    
    def determine_job_type(self, job_text: str) -> str:
        """Determine job type based on job description/title"""
        text_lower = job_text.lower()
        
        remote_keywords = ['remote', 'work from home', 'wfh', 'telecommute', 'distributed']
        hybrid_keywords = ['hybrid', 'flexible', 'part remote', 'mixed']
        
        if any(keyword in text_lower for keyword in remote_keywords):
            return "Remote"
        elif any(keyword in text_lower for keyword in hybrid_keywords):
            return "Hybrid"
        else:
            return "Offline"
    
    def load_more_linkedin_jobs(self, max_pages=5):
        """Load more jobs by clicking 'See more jobs' button"""
        for page in range(max_pages):
            try:
                # Scroll to bottom first
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(3)
                
                # Look for "See more jobs" button
                see_more_selectors = [
                    "button[aria-label='See more jobs']",
                    ".infinite-scroller__show-more-button",
                    ".jobs-search-results__pagination button",
                    "button:contains('See more jobs')"
                ]
                
                button_clicked = False
                for selector in see_more_selectors:
                    try:
                        button = WebDriverWait(self.driver, 5).until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                        )
                        button.click()
                        button_clicked = True
                        time.sleep(random.uniform(3, 7))
                        break
                    except:
                        continue
                
                if not button_clicked:
                    logger.info(f"No more jobs to load at page {page + 1}")
                    break
                    
            except Exception as e:
                logger.error(f"Error loading more jobs: {e}")
                break

    def scrape_linkedin(self) -> List[Job]:
        """Scrape LinkedIn jobs for Saudi Arabia"""
        logger.info("Starting LinkedIn scraping...")
        jobs = []
        total_cards_found = 0
        total_jobs_processed = 0
        
        try:
            # LinkedIn job search URL for Saudi Arabia
            base_url = "https://www.linkedin.com/jobs/search/?keywords={}&location=Saudi%20Arabia&f_TPR=r86400"
            
            for role_index, role in enumerate(self.target_roles):
                logger.info(f"Scraping role {role_index + 1}/{len(self.target_roles)}: '{role}'")
                url = base_url.format(role.replace(' ', '%20'))
                logger.info(f"Navigating to URL: {url}")
                
                self.driver.get(url)
                
                # Wait for page to load completely
                time.sleep(random.uniform(3, 7))
                
                # Scroll and load more jobs
                logger.info("Scrolling to load more jobs...")
                for scroll_attempt in range(10):
                    logger.debug(f"Scroll attempt {scroll_attempt + 1}/3")
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(random.uniform(3, 7))
                
                # Wait for dynamic content to load
                time.sleep(random.uniform(3, 7))
                
                self.load_more_linkedin_jobs(max_pages=10)  
                # Extract job cards
                logger.info("Extracting job cards...")
                job_cards = self.driver.find_elements(By.CSS_SELECTOR, ".job-search-card")
                cards_found = len(job_cards)
                total_cards_found += cards_found
                logger.info(f"Found {cards_found} job cards for role '{role}'")
                
                if cards_found == 0:
                    logger.warning(f"No job cards found for role '{role}'. Trying alternative selectors...")
                    # Try alternative selectors
                    alternative_selectors = [
                        ".base-search-card",
                        ".jobs-search-results__list-item",
                        ".job-result-card",
                        ".jobs-search__results-list li",
                        "[data-job-id]",
                        "[data-entity-urn*='jobPosting']"
                    ]
                    
                    for selector in alternative_selectors:
                        alt_cards = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        if alt_cards:
                            logger.info(f"Found {len(alt_cards)} cards with alternative selector: {selector}")
                            job_cards = alt_cards
                            cards_found = len(job_cards)
                            break
                    
                    if cards_found == 0:
                        continue
                
                # Process job cards (limit to first 20 per role)
                # cards_to_process = min(20, cards_found)
                cards_to_process = cards_found
                logger.info(f"Processing {cards_to_process} job cards...")
                
                for card_index, card in enumerate(job_cards[:cards_to_process]):
                    total_jobs_processed += 1
                    logger.debug(f"Processing card {card_index + 1}/{cards_to_process} for role '{role}'")
                    
                    try:
                        # Wait for element to be visible
                        time.sleep(random.uniform(3, 7))
                        
                        # Extract job title with multiple approaches
                        job_title = ""
                        title_selectors = [
                            ".base-search-card__title",
                            "h3",
                            ".sr-only"
                        ]
                        
                        for selector in title_selectors:
                            try:
                                title_elem = card.find_element(By.CSS_SELECTOR, selector)
                                job_title = title_elem.text.strip()
                                if job_title:
                                    break
                                # Try innerHTML if text is empty
                                job_title = title_elem.get_attribute('innerHTML').strip()
                                if job_title:
                                    # Clean HTML tags
                                    import re
                                    job_title = re.sub(r'<[^>]+>', '', job_title).strip()
                                    break
                            except NoSuchElementException:
                                continue
                        
                        # Extract company name
                        company_name = ""
                        company_selectors = [
                            ".base-search-card__subtitle a",
                            ".base-search-card__subtitle",
                            "h4 a",
                            "h4"
                        ]
                        
                        for selector in company_selectors:
                            try:
                                company_elem = card.find_element(By.CSS_SELECTOR, selector)
                                company_name = company_elem.text.strip()
                                if company_name:
                                    break
                                # Try innerHTML if text is empty
                                company_name = company_elem.get_attribute('innerHTML').strip()
                                if company_name:
                                    import re
                                    company_name = re.sub(r'<[^>]+>', '', company_name).strip()
                                    break
                            except NoSuchElementException:
                                continue
                        
                        # Extract location
                        location = ""
                        location_selectors = [
                            ".job-search-card__location",
                            ".job-result-card__location"
                        ]
                        
                        for selector in location_selectors:
                            try:
                                location_elem = card.find_element(By.CSS_SELECTOR, selector)
                                location = location_elem.text.strip()
                                if location:
                                    break
                                # Try innerHTML if text is empty
                                location = location_elem.get_attribute('innerHTML').strip()
                                if location:
                                    import re
                                    location = re.sub(r'<[^>]+>', '', location).strip()
                                    break
                            except NoSuchElementException:
                                continue
                        
                        # Extract job link
                        job_link = ""
                        link_selectors = [
                            ".base-card__full-link",
                            "a[href*='/jobs/view/']",
                            "a"
                        ]
                        
                        for selector in link_selectors:
                            try:
                                link_elem = card.find_element(By.CSS_SELECTOR, selector)
                                job_link = link_elem.get_attribute("href")
                                if job_link and '/jobs/view/' in job_link:
                                    job_link = job_link.split('?')[0]
                                    break
                            except NoSuchElementException:
                                continue
                        
                        # Extract posting time
                        posted_time = ""
                        time_selectors = [
                            ".job-search-card__listdate--new",
                            "time",
                            "[datetime]"
                        ]
                        
                        for selector in time_selectors:
                            try:
                                time_elem = card.find_element(By.CSS_SELECTOR, selector)
                                posted_time = time_elem.get_attribute("datetime")
                                if posted_time:
                                    break
                            except NoSuchElementException:
                                continue
                        
                        if not posted_time:
                            posted_time = datetime.now().strftime("%Y-%m-%d")
                        
                        # Log extracted data for debugging
                        logger.debug(f"Extracted data:")
                        logger.debug(f"  Title: '{job_title}'")
                        logger.debug(f"  Company: '{company_name}'")
                        logger.debug(f"  Location: '{location}'")
                        logger.debug(f"  Link: '{job_link}'")
                        logger.debug(f"  Posted: '{posted_time}'")
                        
                        # Validate extracted data
                        if not job_title:
                            logger.warning(f"Card {card_index + 1}: Empty job title, skipping")
                            continue
                        if not company_name:
                            logger.warning(f"Card {card_index + 1}: Empty company name, skipping")
                            continue
                        if not job_link:
                            logger.warning(f"Card {card_index + 1}: Empty job link, skipping")
                            continue
                        
                        # Apply filters
                        # if not self.is_relevant_role(job_title):
                        #     logger.info(f"Skipping irrelevant role: '{job_title}'")
                        #     continue
                        
                        if self.is_company_filtered(company_name):
                            logger.info(f"Skipping filtered company: '{company_name}'")
                            continue
                        
                        # Determine job type
                        job_type = self.determine_job_type(f"{job_title} {location}")
                        
                        # Create job object
                        job = Job(
                            company_name=company_name,
                            platform="LinkedIn",
                            job_title=job_title,
                            job_type=job_type,
                            job_link=job_link,
                            posted_time=posted_time,
                            location=location or "Saudi Arabia"
                        )
                        
                        # Check for duplicates
                        job_hash = job.get_hash()
                        
                        # if job_hash not in self.seen_jobs:
                        jobs.append(job)
                        #     self.seen_jobs.add(job_hash)
                        #     logger.info(f"✓ Added job: '{job_title}' at '{company_name}' ({job_type})")
                        # else:
                        #     logger.info(f"Duplicate job found: '{job_title}' at '{company_name}'")
                            
                    except Exception as e:
                        logger.error(f"Card {card_index + 1}: Error processing - {e}")
                        continue
                
                # Add delay between roles
                delay = self.config.get('scraping', {}).get('delay_between_requests', 2)
                logger.debug(f"Waiting {delay} seconds before next role...")
                time.sleep(delay)
                
            logger.info(f"LinkedIn scraping completed. Cards found: {total_cards_found}, Cards processed: {total_jobs_processed}, Jobs extracted: {len(jobs)}")
            
        except Exception as e:
            logger.error(f"LinkedIn scraping failed: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
        
        return jobs
 
    def scrape_bayt(self) -> List[Job]:
        """Scrape Bayt jobs for Saudi Arabia with correct selectors"""
        logger.info("Starting Bayt scraping...")
        jobs = []
        
        try:
            base_url = "https://www.bayt.com/en/saudi-arabia/jobs/{}-jobs/?date=1"
            

            for role in self.target_roles:
                formatted_role = role.replace(' ', '-').lower()
                url = base_url.format(formatted_role)
            
                logger.info(f"Scraping Bayt for role: {role} - URL: {url}")
            
                try:
                    self.driver.get(url)
                    
                    # Wait for page to load completely
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.TAG_NAME, "body"))
                    )
                    
                    # Additional wait for dynamic content
                    time.sleep(random.uniform(3, 7))
                    
                    # Log page title to verify page loaded
                    page_title = self.driver.title
                    logger.info(f"Page loaded: {page_title}")
                    
                    # Find job cards using the correct selector
                    job_cards = self.driver.find_elements(By.CSS_SELECTOR, ".has-pointer-d")
                    logger.info(f"Found {len(job_cards)} job cards")
                    
                    if not job_cards:
                        logger.warning(f"No job cards found for role: {role}")
                        continue
                    
                    # Process job cards
                    for i, card in enumerate(job_cards):
                        try:
                            logger.info(f"Processing job card {i+1}/{len(job_cards)}")
                            
                            # Extract job title and link from h2 > a
                            job_title = None
                            job_link = None
                            try:
                                title_elem = card.find_element(By.CSS_SELECTOR, "h2 a")
                                job_title = title_elem.text.strip()
                                job_link = title_elem.get_attribute("href")
                                if job_link:
                                    job_link = job_link.split('?')[0]
                            except Exception as e:
                                logger.warning(f"Could not extract job title from card {i+1}: {e}")
                                continue
                            
                            # Extract company name from the company link
                            company_name = None
                            try:
                                # Strategy 1: Look for a.t-default.t-bold (original selector)
                                company_elem = card.find_element(By.CSS_SELECTOR, "a.t-default.t-bold")
                                company_name = company_elem.text.strip()
                                logger.info(f"Found company name using a.t-default.t-bold: {company_name}")
                            except Exception as e:
                                logger.warning(f"Strategy 1 failed for card {i+1}: {e}")
                                try:
                                    # Strategy 2: Look for <b> tag in the job-company-location-wrapper
                                    company_elem = card.find_element(By.CSS_SELECTOR, ".job-company-location-wrapper b")
                                    company_name = company_elem.text.strip()
                                    logger.info(f"Found company name using <b> tag: {company_name}")
                                except Exception as e:
                                    logger.warning(f"Strategy 2 failed for card {i+1}: {e}")
                                    
                                    # Strategy 3: Look for any bold text that might be company name
                                    try:
                                        bold_elems = card.find_elements(By.CSS_SELECTOR, "b, .t-bold")
                                        for elem in bold_elems:
                                            text = elem.text.strip()
                                            if (text and text != job_title and 
                                                'Easy Apply' not in text and 
                                                'Saudi nationals' not in text and
                                                'Mid career' not in text and
                                                'Senior' not in text and
                                                'Entry level' not in text):
                                                company_name = text
                                                logger.info(f"Found company name using fallback bold text: {company_name}")
                                                break
                                    except Exception as e:
                                        logger.warning(f"Strategy 3 failed for card {i+1}: {e}")
                                        
                                        # Strategy 4: Parse from card text structure
                                        try:
                                            card_text = card.text
                                            lines = card_text.split('\n')
                                            # Look for company name in the lines after job title
                                            for line in lines[1:]:
                                                line = line.strip()
                                                if (line and line != job_title and 
                                                    not line.startswith('$') and 
                                                    not line.startswith('Yesterday') and 
                                                    not line.startswith('days ago') and 
                                                    'career' not in line.lower() and
                                                    'Easy Apply' not in line and
                                                    'Saudi nationals' not in line and
                                                    'Saudi Arabia' not in line and
                                                    not line.startswith('Seeking')):
                                                    company_name = line
                                                    logger.info(f"Found company name using text parsing: {company_name}")
                                                    break
                                        except Exception as e:
                                            logger.warning(f"Strategy 4 failed for card {i+1}: {e}")

                            # Final validation
                            if not company_name:
                                logger.warning(f"No company name found for card {i+1} - Title: {job_title}")
                                company_name = "Unknown Company"
                            else:
                                logger.info(f"Successfully extracted company name: {company_name}")
                            
                            # Extract location from the div with class "t-mute t-small"
                            location = "Saudi Arabia"
                            try:
                                location_elem = card.find_element(By.CSS_SELECTOR, "div.t-mute.t-small")
                                location_text = location_elem.text.strip()
                                if location_text:
                                    # Extract the city name (before the ·)
                                    location_parts = location_text.split('·')
                                    if len(location_parts) >= 2:
                                        city = location_parts[0].strip()
                                        country = location_parts[1].strip()
                                        location = f"{city}, {country}"
                                    else:
                                        location = location_text
                            except Exception as e:
                                logger.warning(f"Could not extract location from card {i+1}: {e}")
                            
                            # Extract salary if available
                            salary_info = None
                            try:
                                salary_elem = card.find_element(By.CSS_SELECTOR, "dt.jb-label-salary")
                                salary_text = salary_elem.text.strip()
                                if salary_text:
                                    # Remove the icon and extract just the salary range
                                    salary_parts = salary_text.split('$')
                                    if len(salary_parts) > 1:
                                        salary_info = '$' + '$'.join(salary_parts[1:])
                                        logger.info(f"Found salary info: {salary_info}")
                            except Exception:
                                pass
                            
                            # Extract job description
                            description = None
                            try:
                                desc_elem = card.find_element(By.CSS_SELECTOR, "div.jb-descr")
                                description = desc_elem.text.strip()
                            except Exception:
                                pass
                            
                            # Extract career level
                            career_level = None
                            try:
                                career_elem = card.find_element(By.CSS_SELECTOR, "dt.jb-label-careerlevel")
                                career_level = career_elem.text.strip()
                                if career_level:
                                    # Remove the icon text
                                    career_parts = career_level.split()
                                    if len(career_parts) >= 2:
                                        career_level = ' '.join(career_parts[1:])  # Skip the first part (icon)
                            except Exception:
                                pass
                            
                            # Extract posted time
                            posted_time = datetime.now().strftime("%Y-%m-%d")
                            try:
                                date_elem = card.find_element(By.CSS_SELECTOR, "span[data-automation-id='job-active-date']")
                                posted_time_text = date_elem.text.strip()
                                if posted_time_text:
                                    # Convert relative time to actual date
                                    if "Yesterday" in posted_time_text:
                                        posted_time = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
                                    elif "days ago" in posted_time_text:
                                        try:
                                            days = int(posted_time_text.split()[0])
                                            posted_time = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
                                        except Exception:
                                            pass
                                    elif "day ago" in posted_time_text:
                                        posted_time = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
                            except Exception:
                                pass
                            
                            # Validate extracted data
                            if not job_title:
                                logger.warning(f"No job title found for card {i+1}")
                                continue
                            
                            # Apply filters
                            if not self.is_relevant_role(job_title):
                                logger.info(f"Skipping irrelevant role: {job_title}")
                                continue
                                
                            if self.is_company_filtered(company_name):
                                logger.info(f"Skipping filtered company: {company_name}")
                                continue
                            
                            # Determine job type
                            job_type = self.determine_job_type(f"{job_title} {description or ''}")
                            
                            # Create job object
                            job = Job(
                                company_name=company_name,
                                platform="Bayt",
                                job_title=job_title,
                                job_type=job_type,
                                job_link=job_link,
                                posted_time=posted_time,
                                location=location
                            )
                            
                            if salary_info:
                                job.salary_info = salary_info
                            if career_level:
                                job.career_level = career_level
                            if description:
                                job.description = description
                            
                            job_hash = job.get_hash()
                            if job_hash not in self.seen_jobs:
                                jobs.append(job)
                                self.seen_jobs.add(job_hash)
                                logger.info(f"Successfully extracted job: {job_title} at {company_name}")
                            else:
                                logger.info(f"Duplicate job found: {job_title} at {company_name}")
                        
                        except Exception as e:
                            logger.warning(f"Error extracting job card {i+1}: {e}")
                            continue
                    
                    # Add delay between requests
                    time.sleep(self.config.get('scraping', {}).get('delay_between_requests', 2))
                    
                except TimeoutException:
                    logger.error(f"Timeout loading page for role: {role}")
                    continue
                except Exception as e:
                    logger.error(f"Error scraping role {role}: {e}")
                    continue
    
        except Exception as e:
            logger.error(f"Bayt scraping failed: {e}")
        
        logger.info(f"Bayt scraping completed. Found {len(jobs)} jobs.")
        return jobs
    
    def log_script_run(self, total_jobs: int, linkedin_jobs: int, indeed_jobs: int,
                       bayt_jobs: int, remote_jobs: int, hybrid_jobs: int,
                       run_duration: float, status: str):
        """Log script run statistics to Airtable script runs table"""
        try:
            # Use the script runs table ID from the documentation
            base_id = self.config['airtable']['base_id']
            script_runs_url = f"https://api.airtable.com/v0/{base_id}/{self.script_runs_table_id}"
            
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }
            
            # Prepare the record data using field IDs (more reliable than field names)
            record = {
                "fields": {
                    "fldqwll19wwdBpG3M": datetime.now().strftime("%Y-%m-%d"),  # Date field
                    "fld75Ml6lRzi83Iod": total_jobs,                            # Total Jobs Count
                    "fldx5CO059lbEnKIf": linkedin_jobs,                         # LinkedIn Jobs
                    "fld7YAtf2jthC36fY": indeed_jobs,                           # Indeed Jobs
                    "fldogbkGvR3MEFNgC": bayt_jobs,                             # Bayt Jobs
                    "fldTnA12tUjiIUPdn": remote_jobs,                           # Remote Jobs
                    "fldN1l14XK5oY6Go9": hybrid_jobs,                           # Hybrid Jobs
                    "fldaC8LvSCjqvXCrI": run_duration,                          # Run Duration
                    "fldmNEzhSN25NkC1O": status,                                # Status
                    # Note: Run Timestamp is not in the documented fields, so we'll skip it
                }
            }
            
            payload = {"records": [record]}
            
            response = requests.post(script_runs_url, headers=headers, data=json.dumps(payload))
            
            if response.status_code == 200:
                logger.info(f"Successfully logged script run to Airtable: {total_jobs} jobs found")
                return True
            else:
                logger.error(f"Failed to log script run: {response.status_code}, {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error while logging script run: {e}")
            return False

    def save_to_airtable(self, jobs: List[Job]):
        """Save jobs to Airtable in batches"""
        try:
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }
            
            # Prepare job data to send to Airtable
            records = []
            for job in jobs:
                record = {
                    "fields": {
                        "Company Name": job.company_name,
                        "Platform": job.platform,
                        "Job Title": job.job_title,
                        "Job Type": job.job_type,
                        "Job Link": job.job_link,
                        "Posted Time": job.posted_time,
                        "Location": job.location,
                        "Scraped At": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                }
                records.append(record)
            
            # Process records in batches of 10
            batch_size = 10
            total_saved = 0
            
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                payload = {"records": batch}
                
                response = requests.post(self.api_url, headers=headers, data=json.dumps(payload))
                
                if response.status_code == 200:
                    batch_count = len(batch)
                    total_saved += batch_count
                    logger.info(f"Successfully saved batch of {batch_count} jobs to Airtable")
                    
                    # Optional: Add a small delay between batches to avoid rate limiting
                    if i + batch_size < len(records):  # Don't sleep after the last batch
                        time.sleep(0.2)  # 200ms delay
                else:
                    logger.error(f"Failed to save batch to Airtable: {response.status_code}, {response.text}")
                    # Continue with remaining batches even if one fails
            
            logger.info(f"Total jobs saved to Airtable: {total_saved}/{len(jobs)}")
            
        except Exception as e:
            logger.error(f"Error while saving to Airtable: {e}")
   
    def run_scraper(self):
        """Main scraper execution"""
        start_time = time.time()
        logger.info("Starting job scraper for Saudi Arabia...")
        
        try:
            # Setup driver
            self.setup_driver()
            
            # Scrape all platforms
            all_jobs = []
            
            # LinkedIn
            linkedin_jobs = self.scrape_linkedin()
            all_jobs.extend(linkedin_jobs)
            
            # Indeed
            # indeed_jobs = self.scrape_indeed()
            # all_jobs.extend(indeed_jobs)
            
            # Bayt
            bayt_jobs = self.scrape_bayt()
            all_jobs.extend(bayt_jobs)
            
            # Filter out duplicates across platforms
            unique_jobs = []
            # seen_hashes = set()
            
            for job in all_jobs:
                # job_hash = job.get_hash()
                # if job_hash not in seen_hashes:
                unique_jobs.append(job)
                # seen_hashes.add(job_hash)
            
            logger.info(f"Found {len(unique_jobs)} unique jobs after deduplication")
            
            if unique_jobs:
               self.save_to_airtable(unique_jobs)  # Added Airtable save call here
 
            # Calculate metrics
            total_jobs = len(unique_jobs)
            remote_jobs = len([j for j in unique_jobs if j.job_type == 'Remote'])
            hybrid_jobs = len([j for j in unique_jobs if j.job_type == 'Hybrid'])
            linkedin_count = len([j for j in unique_jobs if j.platform == 'LinkedIn'])
            indeed_count = len([j for j in unique_jobs if j.platform == 'Indeed'])
            bayt_count = len([j for j in unique_jobs if j.platform == 'Bayt'])
            run_duration = round(time.time() - start_time, 2)
            
            # Log run statistics to script runs table
            self.log_script_run(
                total_jobs=total_jobs,
                linkedin_jobs=linkedin_count,
                indeed_jobs=indeed_count,
                bayt_jobs=bayt_count,
                remote_jobs=remote_jobs,
                hybrid_jobs=hybrid_jobs,
                run_duration=run_duration,
                status="Success"
            )
            
            # Print summary
            print(f"\n{'='*50}")
            print(f"SCRAPING SUMMARY")
            print(f"{'='*50}")
            print(f"Total jobs found: {total_jobs}")
            print(f"Remote jobs: {remote_jobs}")
            print(f"Hybrid jobs: {hybrid_jobs}")
            print(f"LinkedIn: {linkedin_count}")
            print(f"Indeed: {indeed_count}")
            print(f"Bayt: {bayt_count}")
            print(f"Run duration: {run_duration}s")
            print(f"{'='*50}")
            
            return unique_jobs
            
        except Exception as e:
            run_duration = round(time.time() - start_time, 2)
            logger.error(f"Scraper execution failed: {e}")
            
            # Log failed run
            self.log_script_run(
                total_jobs=0,
                linkedin_jobs=0,
                indeed_jobs=0,
                bayt_jobs=0,
                remote_jobs=0,
                hybrid_jobs=0,
                run_duration=run_duration,
                status="Failed"
            )
            return []
        
        finally:
            if self.driver:
                self.driver.quit()

def main():
    """Main execution function"""
    scraper = JobScraper()
    jobs = scraper.run_scraper()
    return jobs

if __name__ == "__main__":
    main()