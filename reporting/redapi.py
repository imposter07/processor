import os
import sys
import json
import time
import logging
import operator
import calendar
import pandas as pd
import datetime as dt
import selenium.common.exceptions
import reporting.utils as utl
import selenium.common.exceptions as ex
import reporting.vmcolumns as vmc
from selenium.webdriver.common.keys import Keys


class RedApi(object):
    config_path = utl.config_path
    default_config_file_name = 'redapi.json'
    base_url = 'https://ads.reddit.com'
    temp_path = 'tmp'
    base_metric = '//*[@id="metrics.'
    video_metrics = [
        'videoViewableImpressions', 'videoFullyViewableImpressions',
        'videoPlaysWithSound', 'videoPlaysExpanded', 'videoWatches25',
        'videoWatches50', 'videoWatches75', 'videoWatches95', 'videoWatches100',
        'videoWatches3Secs', 'videoWatches10Secs']
    username_str = 'username'
    password_str = 'password'

    def __init__(self, headless=True):
        self.headless = headless
        self.sw = None
        self.browser = None
        self.config_file = None
        self.username = None
        self.password = None
        self.account = None
        self.config_list = None
        self.config = None
        self.key_list = [self.username_str, self.password_str]
        self.aborted = False

    def input_config(self, config):
        logging.info('Loading Reddit config file: {}.'.format(config))
        self.config_file = os.path.join(self.config_path, config)
        self.load_config()
        if not self.aborted:
            self.check_config()

    def load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
            self.username = self.config[self.username_str]
            self.password = self.config[self.password_str]
            self.config_list = [self.username, self.password]
        except IOError:
            logging.error('{} not found.  Aborting.'.format(self.config_file))
            self.aborted = True

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in config file. '
                                ' Aborting.'.format(item))
                self.aborted = True
                break

    def get_data_default_check(self, sd, ed, fields):
        if sd is None:
            sd = dt.datetime.today() - dt.timedelta(days=1)
        if ed is None or ed.date() == dt.datetime.today().date():
            ed = dt.datetime.today() - dt.timedelta(days=1)
        if fields:
            for val in fields:
                if str(val) != 'nan':
                    self.account = val
        return sd, ed

    def sign_in(self, attempt=0):
        logging.info('Signing in.: Attempt {}'.format(attempt))
        login_sel = ['log in', 'sign in']
        login_sel = ["[translate(normalize-space(text()), "
                     "'ABCDEFGHIJKLMNOPQRSTUVWXYZ', "
                     "'abcdefghijklmnopqrstuvwxyz')='{}']".format(x)
                     for x in login_sel]
        try:
            self.sw.click_on_xpath('//*[@id="Content"]/h2/a')
            self.sw.random_delay()
        except ex.NoSuchElementException as e:
            logging.warning('No logo, attempting footer.  Error: {}'.format(e))
            try:
                self.sw.click_on_xpath('//*[@id="Footer"]/p[2]/a')
                self.sw.random_delay()
            except ex.NoSuchElementException as e:
                logging.warning(
                    'No footer, attempting log in link.  Error: {}'.format(e))
                try:
                    self.sw.click_on_xpath('//a{}'.format(login_sel[0]))
                    self.sw.random_delay()
                except ex.NoSuchElementException as e:
                    logging.warning('Could not find Log In, rechecking.'
                                    '  Error: {}'.format(e))
                    self.sw.click_on_xpath("//*{}".format(login_sel[0]))
                    self.browser.switch_to.window(
                        self.browser.window_handles[-1])
        try:
            self.sw.browser.switch_to_alert().accept()
        except selenium.common.exceptions.NoAlertPresentException as e:
            logging.info('No alert: {}'.format(e))
        user_pass = [(self.username, '//*[@id="login-username"]'),
                     (self.password, '//*[@id="login-password"]')]
        for item in user_pass:
            elem = self.browser.find_element_by_xpath(item[1])
            elem.send_keys(item[0])
            self.sw.random_delay(0.3, 1)
            if item[0] == self.password:
                try:
                    elem.send_keys(Keys.ENTER)
                    self.sw.random_delay(1, 2)
                except ex.ElementNotInteractableException:
                    logging.info('Could not find field for {}'.format(item))
                except ex.StaleElementReferenceException:
                    logging.info('Could not find field for {}'.format(item))
        elem_id = 'automation-dashboard-viewSetUp'
        elem_load = self.sw.wait_for_elem_load(elem_id=elem_id)
        if not elem_load:
            logging.warning('{} did not load'.format(elem_id))
            self.sw.take_screenshot(file_name='reddit_error.jpg')
            return False
        error_xpath = '/html/body/div/div/div[2]/div/form/fieldset[2]/div'
        try:
            self.browser.find_element_by_xpath(error_xpath)
            logging.warning('Incorrect password, returning empty df.')
            return False
        except:
            pass
        if self.browser.current_url[:len(self.base_url)] != self.base_url:
            self.sw.go_to_url(self.base_url)
        else:
            logo_xpath = '//*[@id="app"]/div/div/div[1]/div/div[2]/a'
            self.sw.click_on_xpath(logo_xpath, sleep=5)
        if 'adsregister' in self.browser.current_url:
            logging.warning('Could not log in check username and password.')
            return False
        return True

    def set_breakdowns(self):
        logging.info('Setting breakdowns.')
        bd_xpath = '//button[contains(normalize-space(),"Breakdown")]'
        elem_found = self.sw.wait_for_elem_load(elem_id=bd_xpath,
                                                selector=self.sw.select_xpath)
        try:
            self.sw.click_on_xpath(bd_xpath)
        except ex.NoSuchElementException as e:
            msg = 'Could not click elem_found {}: {}'.format(elem_found, e)
            logging.warning(msg)
            self.sw.take_screenshot(file_name='reddit_error.jpg')
        bd_date_xpath = '//button[contains(normalize-space(),"Date")]'
        self.sw.click_on_xpath(bd_date_xpath)

    def get_cal_month(self, lr=1):
        cal_class = 'DayPicker-Caption'
        month = self.browser.find_elements_by_class_name(cal_class)
        month = month[lr - 1].text
        month = dt.datetime.strptime(month, '%B %Y')
        if lr == 2:
            last_day = calendar.monthrange(month.year, month.month)[1]
            month = month.replace(day=last_day)
        return month

    @staticmethod
    def get_comparison(lr=1):
        if lr == 1:
            comp = operator.gt
        else:
            comp = operator.lt
        return comp

    def change_month(self, date, lr, month):
        cal_el = self.browser.find_elements_by_class_name("DayPicker-NavButton")
        cal_el = cal_el[lr - 1]
        month_diff = abs((((month.year - date.year) * 12) +
                          month.month - date.month))
        for x in range(month_diff):
            self.sw.click_on_elem(cal_el, sleep=1)

    def go_to_month(self, date, left_month, right_month):
        if date < left_month:
            self.change_month(date, 1, left_month)
        if date > right_month:
            self.change_month(date, 2, right_month)

    def click_on_date(self, date):
        date = dt.datetime.strftime(date, '%a %b %d %Y')
        cal_date_xpath = "//div[@aria-label='{}']".format(date)
        self.sw.click_on_xpath(cal_date_xpath)

    def find_and_click_date(self, date, left_month, right_month):
        self.go_to_month(date, left_month, right_month)
        self.click_on_date(date)

    def set_date(self, date):
        left_month = self.get_cal_month(lr=1)
        right_month = self.get_cal_month(lr=2)
        self.find_and_click_date(date, left_month, right_month)

    def open_calendar(self, base_xpath):
        cal_button_xpath = '/div/div/div'
        cal_xpath = base_xpath + cal_button_xpath
        self.sw.click_on_xpath(cal_xpath)
        cal_table_xpath = '/html/body/div[8]/div/table/tbody/tr'
        return cal_table_xpath

    def set_dates(self, sd, ed, base_xpath=None):
        logging.info('Setting dates to {} and {}.'.format(sd, ed))
        self.open_calendar(base_xpath)
        self.set_date(sd)
        self.set_date(ed)
        elem = self.browser.find_elements_by_xpath(
            "//*[contains(text(), 'Update')]")
        if len(elem) > 1:
            elem = elem[1]
        else:
            elem = elem[0]
        self.sw.click_on_elem(elem)

    def click_individual_metrics(self):
        for metric in self.video_metrics:
            xpath = '{}{}"]'.format(self.base_metric, metric)
            self.sw.click_on_xpath(xpath, sleep=1)

    def click_grouped_metrics(self):
        xpath = '//span[contains(normalize-space(), "All metrics")]'
        elems = self.sw.browser.find_elements_by_xpath(xpath)
        for elem in elems[1:]:
            elem.click()

    def set_metrics(self):
        logging.info('Setting metrics.')
        columns_xpath = '//div[text()="Columns"]'
        customize_columns_xpath = ('//button[contains(normalize-space(),'
                                   '"Customize Columns")]')
        self.sw.click_on_xpath(columns_xpath)
        self.sw.click_on_xpath(customize_columns_xpath)
        self.click_grouped_metrics()
        apply_button_xpath = '//div[text()="Apply"]'
        self.sw.click_on_xpath(apply_button_xpath)

    def export_to_csv(self):
        logging.info('Downloading created report.')
        utl.dir_check(self.temp_path)
        export_xpath = '//button[contains(normalize-space(), "Export report")]'
        self.sw.click_on_xpath(export_xpath)
        download_xpath = (
            '//button[contains(normalize-space(), "Download .csv")]')
        try:
            self.sw.click_on_xpath(download_xpath)
        except ex.TimeoutException as e:
            logging.warning('Timed out - attempting again. {}'.format(e))
            self.sw.click_on_xpath(download_xpath)

    def get_base_xpath(self):
        base_app_xpath = '//*[@id="app"]/div/div[1]/div[2]/div'
        try:
            self.browser.find_element_by_xpath(base_app_xpath)
        except ex.NoSuchElementException:
            base_app_xpath = base_app_xpath[:-3]
        base_app_xpath += '/'
        return base_app_xpath

    def create_report(self, sd, ed):
        logging.info('Creating report.')
        base_app_xpath = self.get_base_xpath()
        self.set_breakdowns()
        self.set_dates(sd, ed, base_xpath=base_app_xpath)
        self.set_metrics()
        self.export_to_csv()

    def change_account(self):
        drop_class = 'automation-account-name'
        elem = self.browser.find_elements_by_class_name(drop_class)
        elem[0].click()
        account_xpath = '//a[text()="{}"]'.format(self.account)
        self.sw.click_on_xpath(account_xpath)

    def get_data(self, sd=None, ed=None, fields=None):
        self.sw = utl.SeleniumWrapper(headless=self.headless)
        self.browser = self.sw.browser
        sd, ed = self.get_data_default_check(sd, ed, fields)
        sign_in_result = False
        for x in range(3):
            self.sw.go_to_url(self.base_url)
            sign_in_result = self.sign_in(attempt=x + 1)
            if sign_in_result:
                break
        if not sign_in_result:
            self.sw.quit()
            return pd.DataFrame()
        if self.account:
            self.change_account()
        self.create_report(sd, ed)
        df = self.sw.get_file_as_df(self.temp_path)
        self.sw.quit()
        return df

    def check_credentials(self, results, camp_col, success_msg, failure_msg):
        self.sw = utl.SeleniumWrapper(headless=self.headless)
        self.browser = self.sw.browser
        self.sw.go_to_url(self.base_url)
        sign_in_check = self.sign_in()
        self.sw.quit()
        if not sign_in_check:
            msg = ' '.join([failure_msg, 'Incorrect User or password. '
                                         'Check Active and Permissions.'])
            row = [camp_col, msg, False]
            results.append(row)
        else:
            msg = ' '.join(
                [success_msg, 'User or password are corrects:'])
            row = [camp_col, msg, True]
            results.append(row)
        return results

    def test_connection(self, acc_col, camp_col, acc_pre):
        success_msg = 'SUCCESS:'
        failure_msg = 'FAILURE:'
        results = self.check_credentials(
            [], acc_col, success_msg, failure_msg)
        if False in results[0]:
            return pd.DataFrame(data=results, columns=vmc.r_cols)
