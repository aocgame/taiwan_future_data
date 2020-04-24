
import pandas as pd
import os
from configparser import ConfigParser
cfg = ConfigParser()
cfg.read('config.ini')


class SourceEngine:
    """
    Data:2020-04-23
    Auther:win
    Description:使用Pandas拼接原生的多個CSV檔案到一個檔案
    """
    def __init__(self):
        """"""
        Base_Dir = cfg.get('base', 'dir')
        Encoding = r'GB18030'
        # 自行輸入要拼接檔案所在的資料夾完整路徑，不可包含中文
        Pull_Dir = os.path.join(Base_Dir, cfg.get('base', 'pull_dir'))
        # 拚接後要保存的檔案名
        GermFile_Path = os.path.join(Base_Dir, cfg.get('base', 'germ_file'))
        # 選取特定產品來組合資料，TX和後面的空格代表大台期貨產品
        Column_Product_Select = 'TX     '
        # 修改目前工作目錄
        os.chdir(Pull_Dir)
        # 將該資料夾下的所有csv存入一個列表
        file_list = [name for name in os.listdir() if name.endswith('.csv')]

        # 遞迴瀏覽列表中的各個CSV檔案名，並追加到合併後的檔案
        for i in range(0, len(file_list)):
            file_path = os.path.join(Pull_Dir, file_list[i])
            # 讀取第一個CSV檔案並包含表頭
            # 編碼預設UTF-8，為讀取中文更改為 GB18030
            df = pd.read_csv(file_path, encoding=Encoding, low_memory=False)
            df.columns = ['Date', 'Product', 'DueMonth', 'Time', 'Price', 'Volume', 'NearMPrice', 'FarMPrice', 'OpenEx']
            # 選取台指期
            df = df.loc[df['Product'] == Column_Product_Select]
            # 选取主要合约
            df = df.loc[df['DueMonth'] == df['DueMonth'].iloc[0]]
            # 除掉没用的字段
            df = df.loc[:, ['Date', 'Time', 'Price', 'Volume']]
            # 將讀取的第一個CSV檔案寫入合併後的檔案保存
            df.to_csv(
                GermFile_Path,
                encoding=Encoding,
                index=False,
                header=True if i == 0 else False,
                mode='w' if i == 0 else 'a+'
            )
            # 删除csv
            os.remove(file_path)


if __name__ == "__main__":
    SourceEngine()
