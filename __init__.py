__version__ = '1.0.10'

driver = None
geckodriver_location = None

required = [
    'selenium-requests',
    'webdriver-manager',
]

def setup(**kw):
    global geckodriver_location
    try:
        from webdriver_manager.firefox import GeckoDriverManager
        geckodriver_location = GeckoDriverManager().install()
    except Exception as e:
        return False, str(e)
    return True, "Success"

import atexit
def exit_handler():
    global driver
    try:
        driver.quit()
    except:
        pass
atexit.register(exit_handler)

import os, pathlib
from meerschaum.config._paths import PLUGINS_TEMP_RESOURCES_PATH
cookies_path = pathlib.Path(os.path.join(PLUGINS_TEMP_RESOURCES_PATH, 'apex_cookies.pkl'))

xpaths = {
    'initial_login' : "/html/body/div/div/div/main/div/div/div[2]/div/form/button",
    'username' : "/html/body/div/div/div/main/div/div/div[4]/div[1]/form/div[1]/input",
    'password' : "/html/body/div/div/div/main/div/div/div[4]/div[1]/form/div[2]/input",
    'login' : "/html/body/div/div/div/main/div/div/div[4]/div[1]/form/button",
}
urls = {
    'login' : "https://public-apps.apexclearing.com/session/#/login/",
    'activities' : "https://public-api.apexclearing.com/activities-provider/api/v1/activities/",
}
cols_dtypes = {
    'timestamp' : 'datetime64[ns]',
    'accountNumber' : str,
    'accountTitle' : str,
    'symbol' : str,
    'description' : str,
    'descriptionLines' : str,
    'tradeAction' : str,
    'quantity' : float,
    'price' : float,
    'fees' : float,
    'commissions' : float,
    'netAmount' : float,
    'currencyCode' : str,
    'settleDate' : 'datetime64[ns]',
    'tradeDate' : 'datetime64[ns]',
    'tradeNumber' : str,
    'transferDirection' : str,
    'activityType' : str,
    'tagNumber' : str,
    'trailer' : str,
    'accountType' : str,
    'underlyingSymbol' : str,
    'optionType' : str,
    'strikePrice' : float,
    'expirationDate' : 'datetime64[ns]',
}
from meerschaum.utils.debug import dprint
from meerschaum.utils.warnings import warn, error, info

def get_driver(debug : bool = False):
    """
    Returns an alive Firefox WebDriver
    """
    global driver

    ### webdriver with features from the normal requests lib
    from seleniumrequests import Firefox
    ### we need options to start a headless firefox instance
    from selenium.webdriver.firefox.options import Options

    from selenium.webdriver.remote.command import Command
    is_alive = None
    try:
        driver.execute(Command.STATUS)
        is_alive = True
    except:
        is_alive = False

    if not is_alive:
        browser_options = Options()
        browser_options.add_argument('--headless')
        browser_options.add_argument('--window-size=1920x1080')
        driver = Firefox(options=browser_options, executable_path=geckodriver_location)

    ### load existing cookies
    if cookies_path.exists():
        driver.get(urls['login'])
        if debug: dprint("Found existing cookies. Attempting to reuse session...")
        import pickle
        with open(cookies_path, 'rb') as cookies_file:
            cookies = pickle.load(cookies_file)
        for cookie in cookies:
            driver.add_cookie(cookie)

    return driver

def ask_for_credentials():
    """
    Prompt the user for login information and update the Meerschaum configuration file.
    """
    from getpass import getpass
    from prompt_toolkit import prompt
    from meerschaum.utils.formatting._shell import clear_screen
    clear_screen()
    
    def get_password():
        while True:
            password = getpass(prompt="Apex password: ")
            _password = getpass(prompt="Confirm Apex password: ")
            if password != _password:
                warn("Passwords do not match! Try again")
                continue
            else:
                return password

    while True:
        try:
            username = prompt("Apex username: ")
            account  = prompt("Apex account number: ")
            password = get_password()
            break
        except:
            return False

    from meerschaum.config import config as cf
    from meerschaum.config._edit import write_config
    if 'plugins' not in cf: cf['plugins'] = {}
    if 'apex' not in cf['plugins']: cf['plugins']['apex'] = {}
    if 'login' not in cf['plugins']['apex']: cf['plugins']['apex']['login'] = {}
    cf['plugins']['apex']['login']['username'] = username
    cf['plugins']['apex']['login']['password'] = password
    cf['plugins']['apex']['login']['account'] = account
    write_config(cf)
    return username, password, account

def fetch(
        pipe : 'meerschaum.Pipe',
        begin : 'datetime.datetime' = None,
        debug : bool = False,
        **kw
    ) -> 'pd.DataFrame':
    ### SSL fix
    import requests, urllib3
    requests.packages.urllib3.disable_warnings()
    requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS += ':HIGH:!DH:!aNULL'
    try:
      requests.packages.urllib3.contrib.pyopenssl.util.ssl_.DEFAULT_CIPHERS += ':HIGH:!DH:!aNULL'
    except:
      pass

    ### the below imports are needed to wait for elements to load
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.common.by import By
    from selenium.common.exceptions import TimeoutException

    ### modules we'll need later
    import meerschaum as mrsm
    from meerschaum.utils.misc import import_pandas
    pd = import_pandas()
    import datetime
    
    ### get credentials from Meerschaum config or the user
    from meerschaum.config import config as cf, write_config
    while True:
        try:
            apex_username = cf['plugins']['apex']['login']['username']
            apex_password = cf['plugins']['apex']['login']['password']
            apex_account  = cf['plugins']['apex']['login']['account']
        except:
            if ask_for_credentials() is False:
                got_login = False
                break
        else:
            got_login = True
            break

    if not got_login:
        warn(f"Failed to get login information. Aborting...")
        return None

    def apex_login(debug : bool = False):
        import pickle
        driver = get_driver()

        if debug: dprint("Loading login page...")
        driver.get(urls['login'])

        ### enter username on first page
        if debug: dprint("Waiting for first username textbox...")
        WebDriverWait(driver, 6).until(EC.element_to_be_clickable((By.NAME, 'username')))
        initial_login = driver.find_element_by_name("username")
        initial_login.clear()
        initial_login.send_keys(apex_username)
        initial_login.find_element_by_xpath(xpaths['initial_login']).click()

        #### enter username on real login form
        if debug: dprint("Waiting for second username textbox...")
        WebDriverWait(driver, 6).until(EC.element_to_be_clickable((By.XPATH, xpaths['username'])))
        user_login = driver.find_element_by_xpath(xpaths['username'])
        user_login.clear()
        user_login.send_keys(apex_username)

        ### enter password
        if debug: dprint("Waiting for password textbox...")
        WebDriverWait(driver, 6).until(EC.element_to_be_clickable((By.XPATH, xpaths['password'])))
        user_pass = driver.find_element_by_xpath(xpaths['password'])
        user_pass.clear()
        user_pass.send_keys(apex_password)

        ### click login
        if debug: dprint("Clicking login button...")
        WebDriverWait(driver, 6).until(EC.element_to_be_clickable((By.XPATH, xpaths['login'])))
        driver.find_element_by_xpath(xpaths['login']).click()

        ### enter account number and press Enter
        if debug: dprint("Waiting for account textbox...")
        try:
            WebDriverWait(driver, 6).until(EC.element_to_be_clickable((By.ID, 'account')))
        except TimeoutException:
            driver.quit()
            error('Incorrect login. Please check the login information with `mrsm edit config` under plugins:apex:login')
        account_login = driver.find_element_by_id('account')
        account_login.clear()
        account_login.send_keys(apex_account)
        account_login.send_keys(u'\ue007')

        ### save cookies for reuse after main thread has quit
        with open(cookies_path, 'wb') as cookies_file:
            pickle.dump(driver.get_cookies(), cookies_file)

        return True

    def get_activities(
            activity_types : list = ['TRADES', 'MONEY_MOVEMENTS', 'POSITION_ADJUSTMENTS'],
            start_date : datetime.date = None,
            end_date : datetime.date = datetime.date.today(),
            debug : bool = False
        ) -> pd.DataFrame:
        """
        Get activities data from Apex and return a pandas dataframe
        """
        driver = get_driver()
        dfs = []
        if start_date is None: start_date = end_date.replace(year=end_date.year - 2)
        for activity_type in activity_types:
            url = (
                urls['activities'] +
                apex_account +
                f"?activityType={activity_type}" +
                f"&startDate={start_date}" +
                f"&endDate={end_date}"
            )
            if debug: dprint(f"Fetching data from URL: {url}")
            response = driver.request('GET', url)
            df = pd.read_json(response.text)
            ### parse the list column as a string
            dfs.append(df)
        full_df = pd.concat(dfs, ignore_index=True).sort_values(by='timestamp').reset_index(drop=True)
        try:
            full_df['descriptionLines'] = full_df['descriptionLines'].apply(lambda x : "\n".join(x))
        except KeyError:
            pass

        full_df = full_df.astype(cols_dtypes)
        for col, _type in cols_dtypes.items():
            if _type is str:
                ### TODO Figure out why Meerschaum turns empty strings into NULL on the SQL Server
                full_df[col] = full_df[col].replace({'nan' : None, '' : None})

        return full_df

    def main():
        global geckodriver_location

        ### ensure geckodriver is installed (Firefox must be installed externally)
        if geckodriver_location is None:
            success_tuple = setup()
            if not success_tuple[0]: error(success_tuple[1])

        ### init browser with cookies
        driver = get_driver(debug=debug)

        ### determine begin date parameter
        start_date = begin
        if not start_date: start_date = pipe.sync_time
        if start_date: start_date = start_date.date().replace(day=start_date.day - 1)

        ### create and configure the Pipes
        try:
            dt = pipe.columns['datetime']
        except:
            pipe.columns = {'datetime' : 'timestamp'}
            pipe.instance_connector.edit_pipe(pipe, debug=debug)
        running_dividends_pipe = mrsm.Pipe('sql:main', f'running_dividends')
        if running_dividends_pipe.id is None:
            running_dividends_pipe.parameters = {
                'columns' : {
                    'datetime' : 'timestamp',
                },
                'fetch' : {
                    'definition' : f"""
                    SELECT DISTINCT timestamp, sum("netAmount") OVER (ORDER BY timestamp ASC) AS "running_dividends"
                    FROM "{pipe}"
                    WHERE symbol IS NOT NULL AND symbol != ''
                    AND "transferDirection" = 'INCOMING'
                    AND "activityType" = 'MONEY_MOVEMENTS'
                    """,
                },
            }
            running_dividends_pipe.register(debug=debug)

        ### log in to set the session cookies and fetch the dataframe
        try:
            df = get_activities(start_date=start_date, debug=debug)
        except Exception as e:
            info(f"Logging into Apex. Please wait ~10 seconds...")
            if apex_login(debug=debug):
                info("Successfully logged into Apex. This session will be reused until the main thread is stopped.")
            df = get_activities(start_date=start_date, debug=debug)
        return df

    return main()
