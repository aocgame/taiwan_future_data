
import os
import datetime
import zipfile
import urllib3
from configparser import ConfigParser
cfg = ConfigParser()
cfg.read('config.ini')


class DownloadEngine:
    """
    Data:2020-04-23
    Auther:win
    Description:从期交所下载逐笔数据
    Refer:https://www.taifex.com.tw/cht/3/futPrevious30DaysSalesData
    """
    def __init__(self):
        """"""
        Base_Dir = cfg.get('base', 'dir')
        Pull_Dir = os.path.join(Base_Dir, cfg.get('base', 'pull_dir'))
        taifex_url_base = 'https://www.taifex.com.tw/file/taifex/Dailydownload/DailydownloadCSV/'
        if not os.path.exists(Pull_Dir):
            os.makedirs(Pull_Dir)
        os.chdir(Pull_Dir)

        # 下午5点以后才下载今天的数据
        pmcinq = 0 if datetime.datetime.now().hour >= 17 else 1
        for i in range(pmcinq, 20):
            downloaded_date = datetime.date.today() - datetime.timedelta(days=i)
            file_name = 'Daily_' + downloaded_date.strftime('%Y_%m_%d') + '.zip'
            file_already_exists = os.path.exists(file_name)
            if i >= 15:
                # 删除 15 天前的压缩包
                if file_already_exists:
                    os.remove(file_name)
            elif not file_already_exists:
                url = taifex_url_base + file_name
                self.download_file(url, file_name)
                filesize = os.path.getsize(file_name)
                if filesize > float(1024 * 200):
                    # 大于200kb就解压缩
                    zip_file = zipfile.ZipFile(file_name)
                    zip_file.extractall()
                    zip_file.close()
                    print('解压缩' + file_name)

    def download_file(self, url, file):
        try:
            http = urllib3.PoolManager()
            req = http.request('GET', url)
            with open(file, 'wb') as zip_file:
                zip_file.write(req.data)
            req.release_conn()
        except urllib3.exceptions.HTTPError as e:
            print('Request failed:', e.reason)


if __name__ == "__main__":
    DownloadEngine()
