import sys
import socket
import random
import time
import requests
import os
import json
import re

token = os.getenv('TOKEN')


TCP_IP = "0.0.0.0"
TCP_PORT = 9999
conn = None

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)

conn, addr = s.accept()

lang = ["Python", "Java", "JavaScript"]

while True:
    try:
        url = 'https://api.github.com/search/repositories?q=+language:{$Programming Language}&sort=updated&order=desc&per_page=50'
        res = requests.get(url, headers={"Authorization": token })
        result_json = res.json()
        for i in result_json["items"]:
            if i["language"] in lang and i["description"] is None:
                info = {"full_name": i["full_name"], "language": i["language"], "stargazers_count": i["stargazers_count"], "description": i["description"], "pushed_at": re.sub("Z","", re.sub("T"," ",i["pushed_at"]))}
                data = f"{json.dumps(info)}\n".encode()
                conn.send(data)
                print(info)
            elif i["language"] in lang:
                info = {"full_name": i["full_name"], "language": i["language"], "stargazers_count": i["stargazers_count"], "description": re.sub("[^a-zA-z ]", "", i["description"]), "pushed_at": re.sub("Z","", re.sub("T"," ",i["pushed_at"]))}
                data = f"{json.dumps(info)}\n".encode()
                conn.send(data)
                print(info)
        time.sleep(15)
    except KeyboardInterrupt:
        s.shutdown(sock.SHUT_RD)
