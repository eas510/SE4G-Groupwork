import subprocess
import time
import requests

# 启动webserver.py
webserver_process = subprocess.Popen(['python', 'webserver.py'])

# 检查服务器是否启动
server_started = False
server_url = 'http://localhost:5000'  # 根据你的服务器URL和端口修改

while not server_started:
    try:
        response = requests.get(server_url)
        if response.status_code == 200:
            server_started = True
    except requests.exceptions.ConnectionError:
        time.sleep(1)

# 服务器启动后，启动dashboard.ipynb
notebook_process = subprocess.Popen(['jupyter', 'notebook', 'dashboard.ipynb'])

try:
    # 等待服务器和笔记本运行，直到用户中断
    webserver_process.wait()
    notebook_process.wait()
except KeyboardInterrupt:
    # 捕获中断信号并关闭所有进程
    webserver_process.terminate()
    notebook_process.terminate()
