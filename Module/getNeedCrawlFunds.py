import requests
from selenium import webdriver
from abc import abstractmethod, ABC
from typing import Optional, NoReturn


class NeedCrawledFundModule(ABC):
    """
    基金爬取任务模块
    通过生成器逐个给出 需要爬取的基金
    """

    class NeedCrawledOnceFund:
        """
        需要爬取的 单个基金信息
        """

        def __init__(self, code: str, name: str):
            self.code = code
            self.name = name

    def __init__(self):
        self.total = None
        self.task_generator: Optional[Generator[NeedCrawledFundModule.NeedCrawledOnceFund]] = None

        self.init_generator()

    @abstractmethod
    def init_generator(self) -> NoReturn:
        """
        初始化 生成器
        """
        return NotImplemented


class GetNeedCrawledFundByWeb(NeedCrawledFundModule):

    def init_generator(self) -> NoReturn:
        # 全部（不一定可购） 的开放式基金
        url = 'http://fund.eastmoney.com/data/fundranking.html#tall;c0;r;s1nzf;pn50;ddesc;qsd20220730;qed20230730;' \
              'qdii;zq;gg;gzbd;gzfs;bbzt;sfbb'
        page = requests.get(url, headers={
            "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/78.0.3904.108 Safari/537.36'})
        co = webdriver.ChromeOptions()
        # 是否有浏览界面，False：有；True：无
        co.headless = True
        # chrome_driver路径
        # chrome_driver = r'D:\temp\chromedriver_win32\chromedriver.exe'

        co.add_argument('--headless')

        # options.add_argument(user_agent)
        browser = webdriver.Chrome(options=co)
        # 基金排行的url
        browser.get(url)
        body = browser.find_element_by_tag_name('body')
        mainframe = body.find_element_by_xpath("(//div[@class='mainFrame'])[7]")

        table = mainframe.find_element_by_class_name('dbtable')
        tbody = table.find_element_by_tag_name('tbody')
        funds = tbody.find_elements_by_tag_name('tr')

        fund_list = []
        for fund in funds:
            fund_code = fund.find_element_by_css_selector('td:nth-child(3)')
            fund_name = fund.find_element_by_css_selector('td:nth-child(4)')
            # 通过CSS样式选择器选择第n个td标签
            day_increase = fund.find_element_by_css_selector('td:nth-child(8)')
            week_increase = fund.find_element_by_css_selector('td:nth-child(9)')
            month_increase = fund.find_element_by_css_selector('td:nth-child(10)')
            quarter_increase = fund.find_element_by_css_selector('td:nth-child(11)')
            half_year_increase = fund.find_element_by_css_selector('td:nth-child(12)')
            year_increase = fund.find_element_by_css_selector('td:nth-child(13)')
            two_year_increase = fund.find_element_by_css_selector('td:nth-child(14)')
            three_year_increase = fund.find_element_by_css_selector('td:nth-child(14)')
            if year_increase > 0 and (True if two_year_increase is None else (two_year_increase > 0)) \
                    and (True if three_year_increase is None else (three_year_increase > 0)):
                fund_list.append(NeedCrawledFundModule.NeedCrawledOnceFund(fund_code, fund_name))

        self.total = len(fund_list)

        self.task_generator = fund_list
