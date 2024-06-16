from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service as ChromeService
# from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from selenium.webdriver.chrome.service import Service

import time
import random

# from selenium import webdriver
# driver = webdriver.Chrome()
# from selenium import webdriver
# from selenium.webdriver.firefox.service import Service
# # service = Service(executable_path='/home/anon/weekly-projects/scrapy_engine/server/geckodriver')
# service = Service(executable_path='/home/ubuntu/geckodriver')
# options = webdriver.FirefoxOptions()
# options.add_argument('--headless')
# options.add_argument('--disable-gpu')
# options.add_argument('--no-sandbox')
# options.add_argument('enable-automation')
# options.add_argument('--disable-infobars')
# options.add_argument('--disable-dev-shm-usage')
# driver = webdriver.Firefox(service=service, options=options)
# driver = webdriver.Firefox(options=options)




# # https://googlechromelabs.github.io/chrome-for-testing/
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
# service = Service(executable_path='/home/anon/weekly-projects/scrapy_engine/server/geckodriver')
service = Service(executable_path='/usr/bin/chromedriver')
options = webdriver.ChromeOptions()
options.add_argument('--headless')
options.add_argument('--disable-gpu')
options.add_argument('--no-sandbox')
options.add_argument('enable-automation')
options.add_argument('--disable-infobars')
options.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome(service=service, options=options)
# driver = webdriver.Chrome(options=options)







# Helper functions
def set_random_delay(min_seconds=3, max_seconds=8):
    random_delay = random.uniform(min_seconds, max_seconds)
    time.sleep(random_delay)
    print(f"Waiting for {random_delay} seconds before closing the browser...")

def take_screenshot(driver, path):
    print(path.split('.png')[:-1])
    driver.save_screenshot(path)


'''
from selenium import webdriver

from selenium.webdriver.chrome.options import Options
driver = webdriver.Chrome()

driver.get('https://github.com/')
print(driver.title)

'''

def get_it_running(username, password, some_url, jobs_page, clone_index=6, run_index=8):
    # Set up Chrome options
    # chrome_options = webdriver.ChromeOptions()
    # chrome_options.add_argument('--no-sandbox')
    # chrome_options.add_argument('--start-maximized')
    # chrome_options.add_argument('--lang=en-US,en;q=0.9')
    # chrome_options.add_argument('--headless')               # headless

    # Initialize the Chrome driver
    # driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)
    driver = webdriver.Firefox()

    # run headless /usr/bin/chromium-browser
    # webdriver.Chrome("/usr/lib/chromium-browser/chromedriver")

    # service = Service(executable_path='/usr/bin/chromium-browser')
    # options = webdriver.ChromeOptions()
    # driver = webdriver.Chrome(service=service, options=chrome_options)
    
    # Go to the URL
    driver.get(some_url)

    # Capture screenshot
    take_screenshot(driver, 'the_homepage.png')

    # Accept cookies
    try:
        accept_cookies_button = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.ID, 'onetrust-accept-btn-handler'))
        )
        accept_cookies_button.click()
        take_screenshot(driver, 'cookies.png')
        print('accepting cookies')
    except Exception as e:
        print(f"Error accepting cookies: {e}")

    # Enter username
    username_field = driver.find_element(By.ID, 'id_username')
    username_field.send_keys(username)
q
    # Enter password
    password_field = driver.find_element(By.ID, 'id_password')
    password_field.send_keys(password)

    # Take screenshot after entering password
    take_screenshot(driver, 'entered_password.png')

    # press enter key
    password_field.send_keys(Keys.RETURN)

    # sleep for 8 to 12 seconds
    set_random_delay(8,12)


    # # Click sign in with email
    # try:
    #     sign_in_button = WebDriverWait(driver, 10).until(
    #         EC.visibility_of_element_located((By.CLASS_NAME, 'mat-ripple'))
    #     )
    #     sign_in_button.click()
    #     take_screenshot(driver, 'signed_in.png')
    #     print('signing in')
    # except Exception as e:
    #     print(f"Error signing in: {e}")

    # Navigate to the jobs page
    driver.get(jobs_page)
    
    # Wait and click on the 6th element with class 'checkbox-custom'
    try:
        checkbox_custom = WebDriverWait(driver, 10).until(
            EC.visibility_of_all_elements_located((By.CLASS_NAME, 'checkbox-custom'))
        )
        take_screenshot(driver, 'opened_jobs_page.png')
        print('opened jobs page')
        checkbox_custom[5].click()
        print('select project to clone.')
        take_screenshot(driver, 'checkbox.png')
    except Exception as e:
        print(f"Error selecting project: {e}")
    
    set_random_delay()
    # Click clone
    try:
        btn_content = WebDriverWait(driver, 10).until(
            EC.visibility_of_all_elements_located((By.CLASS_NAME, 'btn-content'))
        )
        btn_content[clone_index].click()
        take_screenshot(driver, 'clone.png')
    except Exception as e:
        print(f"Error clicking clone: {e}")

    # wait for 5 seconds
    set_random_delay()

    # Click run
    try:
        run_button = WebDriverWait(driver, 10).until(
            EC.visibility_of_all_elements_located((By.CLASS_NAME, 'mat-ripple'))
        )
        run_button[run_index].click()
        take_screenshot(driver, 'clicked_run.png')
        print('clicked_run.png')
    except Exception as e:
        print(f"Error clicking run: {e}")

    # Generate a random delay between 5 to 10 seconds
    set_random_delay(5, 10)
    

    # Wait for the random delay
    time.sleep(delay_seconds)

    # Take screenshot before closing
    take_screenshot(driver, 'browser_close.png')

    # Close the browser
    driver.quit()



configs = [
#   {'username':'aanandaprashadgiri@yahoo.com', 'password':'4"#aMN2!b9q->f-', 'jobs_page':'https://app.zyte.com/p/753046/jobs', 'clone_index':6, 'run_index':8}, # checked
#   {'username':'aananda.giri@yahoo.com', 'password':'4"#aMN2!b9q->f-', 'jobs_page':'https://app.zyte.com/p/753047/jobs', 'clone_index':6, 'run_index':8},     # checked
  {'username':'mokinjay@protonmail.com', 'password':'4"#aMN2!b9q->f-', 'jobs_page':'https://app.zyte.com/p/753064/jobs', 'clone_index':6, 'run_index':8},     # checked
#   {'username':'aanandaprashadgiri@proton.me', 'password':'4"#aMN2!b9q->f-', 'jobs_page':'https://app.zyte.com/p/753065/jobs', 'clone_index':6, 'run_index':8},     # checked
#   {'username':'076bei001.aananda@proton.me', 'password':'4"#aMN2!sfad3->f-', 'jobs_page':'https://app.zyte.com/p/753079/jobs', 'clone_index':6, 'run_index':8},     # checked
#   {'username':'int.aananda@proton.me', 'password':'kfjdioer324#$F', 'jobs_page':'https://app.zyte.com/p/753077/jobs', 'clone_index':6, 'run_index':8},     # checked
#   {'username':'aananda.giri@proton.me', 'password':'faF9;fkslda£$#', 'jobs_page':'https://app.zyte.com/p/753083/jobs', 'clone_index':6, 'run_index':8},  # checked
#   {'username':'Aananda.giri@yandex.com', 'password':'faF9;fkdewslda£$#', 'jobs_page':'https://app.zyte.com/p/753088/jobs', 'clone_index':6, 'run_index':8},  # checked
#   {'username':'kritrim@proton.me', 'password':'fafsd52437aF9;fkdewslda£$#', 'jobs_page':'https://app.zyte.com/p/753479/jobs', 'clone_index':6, 'run_index':8}, # checked

]

for config in configs:
    get_it_running(config['username'], config['password'], 'https://app.zyte.com/login', config['jobs_page'], config['clone_index'], config['run_index'])