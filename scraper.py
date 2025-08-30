import asyncio
import aiohttp
import csv
import json
import os
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import random

load_dotenv()

class JobSearcherScraper:
    def __init__(self):
        self.base_url = "https://1is.az"
        self.login_page_url = f"{self.base_url}/login"
        self.login_post_url = f"{self.base_url}/loginu"
        self.login_email = os.getenv('login')
        self.login_password = os.getenv('password')
        self.session = None
        
    async def create_session(self):
        connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)
        timeout = aiohttp.ClientTimeout(total=30)
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
        )
        
    async def login(self):
        try:
            # First get login page to get CSRF token
            async with self.session.get(self.login_page_url) as response:
                login_page = await response.text()
                soup = BeautifulSoup(login_page, 'html.parser')
                
                # Find CSRF token from hidden input
                csrf_token = None
                csrf_input = soup.find('input', {'name': '_token'})
                if csrf_input:
                    csrf_token = csrf_input.get('value')
                    print(f"Found CSRF token: {csrf_token[:20]}...")
                
                if not csrf_token:
                    print("No CSRF token found!")
                    return False
                
            # Prepare login data
            login_data = {
                '_token': csrf_token,
                'email': self.login_email,
                'password': self.login_password,
            }
            
            # Prepare headers
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Referer': self.login_page_url,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            }
            
            print(f"Attempting login with email: {self.login_email}")
            print(f"Using endpoint: {self.login_post_url}")
            
            # Submit login form
            async with self.session.post(self.login_post_url, data=login_data, headers=headers, allow_redirects=True) as response:
                response_text = await response.text()
                final_url = str(response.url)
                
                print(f"Login response status: {response.status}")
                print(f"Final URL: {final_url}")
                
                # Check for successful login indicators
                # If redirected away from login page or contains logout, likely successful
                if (response.status in [200, 302] and 
                    (final_url != self.login_page_url and 
                     final_url != self.login_post_url and
                     ('logout' in response_text.lower() or 
                      'dashboard' in response_text.lower() or
                      'profile' in response_text.lower() or
                      'jobsearcher' in response_text.lower()))):
                    print("Login successful!")
                    return True
                else:
                    print("Login failed - checking response")
                    # Check if we're still on login page with error
                    if 'login' in final_url.lower():
                        print("Still on login page - credentials may be invalid")
                        if 'error' in response_text.lower() or 'invalid' in response_text.lower():
                            print("Error message detected in response")
                    print(f"Response preview: {response_text[:300]}")
                    return False
                    
        except Exception as e:
            print(f"Login error: {e}")
            import traceback
            traceback.print_exc()
            return False
            
    async def extract_candidate_data(self, candidate_id):
        url = f"{self.base_url}/jobsearcher/{candidate_id}"
        
        try:
            async with self.session.get(url) as response:
                if response.status != 200:
                    return None
                    
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                # Extract candidate data
                data = {
                    'id': candidate_id,
                    'name': '',
                    'job_field': '',
                    'experience': '',
                    'education_level': '',
                    'min_salary': '',
                    'education_details': '',
                    'skills': '',
                    'work_experience': '',
                    'phone': '',
                    'email': '',
                    'profile_image': ''
                }
                
                # Name - find the correct h3 in jobsearcher-text
                name_element = soup.find('div', class_='jobsearcher-text')
                if name_element:
                    h3 = name_element.find('h3')
                    if h3:
                        data['name'] = h3.get_text(strip=True)
                else:
                    # Fallback to any h3
                    h3_elements = soup.find_all('h3')
                    for h3 in h3_elements:
                        text = h3.get_text(strip=True)
                        if text and 'İş axtaranlar' not in text:
                            data['name'] = text
                            break
                
                # Profile image
                avatar_img = soup.find('div', class_='jobsearcher-avatar')
                if avatar_img:
                    img = avatar_img.find('img')
                    if img:
                        data['profile_image'] = img.get('src', '')
                
                # Job information sections
                job_info_divs = soup.find_all('div', class_='job-information')
                for div in job_info_divs:
                    h5 = div.find('h5')
                    p = div.find('p')
                    if h5 and p:
                        label = h5.get_text(strip=True)
                        value = p.get_text(strip=True)
                        
                        if 'İşləyəcəyi sahə' in label:
                            data['job_field'] = value
                        elif 'Təcrübə' in label:
                            data['experience'] = value
                        elif 'Təhsil' in label:
                            data['education_level'] = value
                        elif 'Minimum əmək haqqı' in label:
                            data['min_salary'] = value
                        elif 'Əlaqə Telefonu' in label:
                            data['phone'] = value
                        elif 'Email' in label:
                            email_link = div.find('a')
                            if email_link:
                                data['email'] = email_link.get_text(strip=True)
                
                # Education details
                education_section = soup.find('header', string='Təhsil')
                if education_section:
                    education_parent = education_section.find_parent()
                    if education_parent:
                        education_text = education_parent.find('p')
                        if education_text:
                            data['education_details'] = education_text.get_text(strip=True)
                
                # Skills
                skills_section = soup.find('header', string='Bacarıqlar')
                if skills_section:
                    skills_parent = skills_section.find_parent()
                    if skills_parent:
                        skills_div = skills_parent.find('div', class_='jobsearcher-ability')
                        if skills_div:
                            skills_p = skills_div.find('p')
                            if skills_p:
                                data['skills'] = skills_p.get_text(strip=True)
                
                # Work experience
                experience_section = soup.find('header', string='Təcrübə')
                if experience_section:
                    exp_parent = experience_section.find_parent()
                    if exp_parent:
                        exp_p = exp_parent.find('p')
                        if exp_p:
                            data['work_experience'] = exp_p.get_text(strip=True)
                
                return data
                
        except Exception as e:
            print(f"Error scraping candidate {candidate_id}: {e}")
            return None
            
    async def scrape_candidates(self, start_id=1, end_id=1000, batch_size=10):
        if not self.session:
            await self.create_session()
            
        # Login first
        if not await self.login():
            print("Failed to login. Exiting.")
            return
            
        all_candidates = []
        
        # Process in batches to avoid overwhelming the server
        for batch_start in range(start_id, end_id + 1, batch_size):
            batch_end = min(batch_start + batch_size - 1, end_id)
            batch_ids = range(batch_start, batch_end + 1)
            
            print(f"Processing batch {batch_start}-{batch_end}")
            
            # Create tasks for current batch
            tasks = [self.extract_candidate_data(candidate_id) for candidate_id in batch_ids]
            
            # Execute batch
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            for result in batch_results:
                if isinstance(result, dict):
                    all_candidates.append(result)
                    print(f"Scraped candidate {result['id']}: {result['name']}")
                elif isinstance(result, Exception):
                    print(f"Error in batch: {result}")
            
            # Add delay between batches to be respectful
            await asyncio.sleep(random.uniform(1, 3))
            
        return all_candidates
        
    async def save_to_csv(self, candidates, filename='candidates.csv'):
        if not candidates:
            print("No candidates to save")
            return
            
        fieldnames = ['id', 'name', 'job_field', 'experience', 'education_level', 
                     'min_salary', 'education_details', 'skills', 'work_experience', 
                     'phone', 'email', 'profile_image']
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(candidates)
            
        print(f"Saved {len(candidates)} candidates to {filename}")
        
    async def save_to_json(self, candidates, filename='candidates.json'):
        if not candidates:
            print("No candidates to save")
            return
            
        with open(filename, 'w', encoding='utf-8') as jsonfile:
            json.dump(candidates, jsonfile, ensure_ascii=False, indent=2)
            
        print(f"Saved {len(candidates)} candidates to {filename}")
        
    async def close_session(self):
        if self.session:
            await self.session.close()

async def main():
    scraper = JobSearcherScraper()
    
    try:
        # Scrape candidates from 1 to 1000
        candidates = await scraper.scrape_candidates(1, 1000, batch_size=5)
        
        # Save results
        if candidates:
            await scraper.save_to_csv(candidates)
            await scraper.save_to_json(candidates)
            print(f"Scraping completed. Total candidates scraped: {len(candidates)}")
        else:
            print("No candidates scraped")
        
    except KeyboardInterrupt:
        print("Scraping interrupted by user")
    except Exception as e:
        print(f"Scraping error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await scraper.close_session()

if __name__ == "__main__":
    asyncio.run(main())