# import pickle
# import requests
# import time
# from selenium import webdriver
#
#
# def download_cookies(file_name):
#     driver = webdriver.Chrome(executable_path="./chromedriver")
#     driver.get("https://github.com/login")
#     time.sleep(0.5)
#     driver.find_element_by_id("login_field").send_keys("phchcc@gmail.com")
#     driver.find_element_by_id("password").send_keys("phc,gher1")
#     driver.find_element_by_name("commit").click()
#     time.sleep(0.5)
#     driver.get("https://github.com/trending")
#     cookies = driver.get_cookies()
#     pickle.dump(cookies, open(file_name, "wb"))
#
#
# def get_session(file_name):
#     cookies = pickle.load(open(file_name, "rb"))
#     session = requests.Session()
#     for cookie in cookies:
#         # print(cookie['name'], cookie['value'])
#         session.cookies.set(cookie['name'], cookie['value'])
#     return session
#
#
# if __name__ == '__main__':
#     download_cookies("cookies.pkl")
#     # get_session("cookies.pkl")
