import paramiko

HOST = '192.168.128.130'
USER = 'jay'
PWD = 'qwe123qwe'

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(HOST, username=USER, password=PWD, timeout=15, allow_agent=False, look_for_keys=False)
print('Connected')

sftp = client.open_sftp()

files = [
    (r'D:\桌面\dataBase_03\dataBase\src\python\finance_spark_streaming.py',
     '/home/jay/dataBase_03/dataBase/src/python/finance_spark_streaming.py'),
    (r'D:\桌面\dataBase_03\dataBase\src\python\finance_api_server.py',
     '/home/jay/dataBase_03/dataBase/src/python/finance_api_server.py'),
    (r'D:\桌面\dataBase_03\dataBase\output\finance_dashboard.html',
     '/home/jay/dataBase_03/dataBase/output/finance_dashboard.html'),
]

for local, remote in files:
    sftp.put(local, remote)
    size = sftp.stat(remote).st_size
    print(f'OK: {remote.rsplit("/",1)[-1]} ({size/1024:.1f}KB)')

sftp.close()

# Verify syntax
_, out, _ = client.exec_command(
    'cd /home/jay/dataBase_03/dataBase && '
    'python3 -c "import ast; ast.parse(open(\"src/python/finance_spark_streaming.py\").read()); print(\"Spark syntax OK\")" && '
    'python3 -c "import ast; ast.parse(open(\"src/python/finance_api_server.py\").read()); print(\"API syntax OK\")"'
)
print(out.read().decode())

client.close()
print('Done')
