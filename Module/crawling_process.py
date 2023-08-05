from concurrent.futures import Future, ThreadPoolExecutor
from enum import Enum, auto, unique
from multiprocessing import Queue, Process, Event, synchronize
from os import cpu_count
from queue import Empty
from sys import maxsize
from time import sleep
from typing import Optional, NoReturn

from bs4 import BeautifulSoup
from requests import Response as RequestsResponse, RequestException, get



class ProcessCrawling():
    """
    独立爬取进程
    内部维护了一个线程池 来进行请求的爬取
    """

    def __init__(self, request_queue: Queue, result_queue: Queue, exit_sign: synchronize.Event):
        super().__init__()
        # 和父进程之间的通信
        self._request_queue = request_queue
        self._result_queue = result_queue
        self._exit_sign = exit_sign

        # 爬取速率控制
        self._rate_control = RateControl()

    @staticmethod
    def get_page(request: Request) -> Response:
        """
        页面下载
        """
        # header = {"User-Agent": singleton_fake_ua.get_random_ua()}
        # try:
        #     page = get(request.url, headers=header, timeout=1)
        #     if page.status_code != 200 or not page.text:
        #         # 反爬虫策略之 给你返回空白的 200结果
        #         raise AttributeError
        #     return Response(request, Response.State.SUCCESS, page)
        # except (RequestException, AttributeError):
        #     return Response(request, Response.State.FALSE, None)
    def create_request(fund_code, page, per, sdate, edate):
            # https://fundf10.eastmoney.com/F10DataApi.aspx?type=lsjz&code=050026&page=1&sdate=2020-01-01&edate=2020-03-01&per=20

        url = "https://fundf10.eastmoney.com/F10DataApi.aspx?"
        data = {
                "type": "lsjz",
                "code": fund_code,
                "page": page,
                "per": per,
                "sdate": sdate,
                "edate": edate
        }
        headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
        }
        response = requests.get(url=url, params=data, headers=headers)
            response.encoding = "utf-8"
            return response

        # 解析源码
    def parse_html(response):
        soup = BeautifulSoup(response.text, "lxml")
        # 先解析获取所有tr标签包含的内容，是一个列表
        trs = soup.find_all("tr")
        # 最终解析完毕后的结果，初始是一个空列表，后续解析逐渐加入
        result = []
        for tr in trs[1:]:
            fund_date = tr.find_all("td")[0].text  # 净值日期
            fund_nav = tr.find_all("td")[1].text  # 单位净值
            fund_accnav = tr.find_all("td")[2].text  # 累计净值
            fund_dgr = tr.find_all("td")[3].text  # 日增长率
            subsribe_state = tr.find_all("td")[4].text  # 申购状态
            redeem_state = tr.find_all("td")[5].text  # 赎回状态
            result.append([fund_date, fund_nav, fund_accnav, fund_dgr, subsribe_state, redeem_state])
        return result

    def run(self) -> NoReturn:
        """
        爬取主流程
        """
        executor = ThreadPoolExecutor()
        future_list: list[Future] = []
        need_retry_task_list: list[Request] = list()

        # 爬取过程记录，用于调优，平时可注释
        # self._rate_control.start_analyze()

        while True:
            # 爬取结束
            if self._exit_sign.is_set() and self._request_queue.empty() and not future_list \
                    and not need_retry_task_list:
                executor.shutdown()
                self._rate_control.shutdown()
                self._result_queue.close()
                self._exit_sign.clear()
                break

            # 获取已完成的task
            need_handle_result_list: list[Response] = list()
            for future in future_list:
                if future.done():
                    result: Response = future.result()
                    need_handle_result_list.append(result)
                    future_list.remove(future)
                    continue

            # 处理爬取结果
            for result in need_handle_result_list:
                if result.state == Response.State.FALSE and result.request.retry_time > 0:
                    # 失败重试
                    result.request.retry_time -= 1
                    need_retry_task_list.append(result.request)
                    continue
                self._result_queue.put(result)

            # 爬取速率控制
            success_count = sum(
                [1 if result.state == Response.State.SUCCESS else 0 for result in need_handle_result_list])
            number_of_concurrent_tasks = self._rate_control \
                .get_cur_number_of_concurrent_tasks(success_count, len(need_handle_result_list) - success_count)

            # 处理爬取请求
            while (not self._request_queue.empty() or len(need_retry_task_list) > 0) \
                    and number_of_concurrent_tasks > len(future_list):
                # 优先处理需要重试的任务
                request = need_retry_task_list.pop() if len(need_retry_task_list) > 0 else self._request_queue.get()
                future_list.append(executor.submit(self.get_page, request))
                number_of_concurrent_tasks -= 1

            # 休眠主线程，避免循环占用过多的cpu时间
            sleep(0.1)

        # 确保数据都写入后，再退出主线程
        # OS pipes are not infinitely long, so the process which queues data could be blocked in the OS during the
        # put() operation until some other process uses get() to retrieve data from the queue
        self._result_queue.join_thread()





