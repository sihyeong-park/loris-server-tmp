"""
FTP 폴더와 구글 드라이브 폴더 연동을 시켜주는 작업 (스케줄러를 걸어서 주기적으로 자동 실행하기)
"""
import os
import shutil

gd_path = {
    'gd_tick_path': 'G:\\Shared drives\\Project_TBD\\Stock_Data\\real_time\\kiwoom_data',
    'gd_minute_path': 'G:\\Shared drives\\Project_TBD\\Stock_Data\\qraft\\minute',
    'gd_daily_path': 'G:\\Shared drives\\Project_TBD\\Stock_Data\\fnguide'
}

server_path = {
    'server_tick_path': 'C:\\Users\\qraft-rp\\Desktop\\FTP\\tick',
    'server_minute_path': 'C:\\Users\\qraft-rp\\Desktop\\FTP\\minute',
    'server_daily_path': 'C:\\Users\\qraft-rp\\Desktop\\FTP\\daily'
}


if __name__ == '__main__':
    for data_type in ['tick', 'minute', 'daily']:
        gd = gd_path[f'gd_{data_type}_path']
        server = server_path[f'server_{data_type}_path']

        gd_files = os.listdir(gd)
        server_files = os.listdir(server)

        if data_type == 'tick':
            gd_files = [f for f in gd_files if '.zst' in f]

        if data_type == 'daily':
            gd_files = [f for f in gd_files if 'csv' in f]

        to_move_files = list(set(gd_files) - set(server_files))

        for f in to_move_files:
            shutil.move(f'{gd}\\{f}', f'{server}\\{f}')
            print(f'{data_type} {server}\\{f}')