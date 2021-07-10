import json
import subprocess

#Read cofig.file 
with open('config.json') as f:
    data = json.load(f)

try:
    command = '''sqoop import --connect {connectionstr} --username {id} --password {passwrd} --table {tname} \
    --target-dir {trgtDir} --hive-import --create-hive-table --hive-table {dbname}'''.format(connectionstr=data['conn'],
                                                                                             id=data['username'],
                                                                                             passwrd=data['password'],
                                                                                             tname=data['tname'],
                                                                                             trgtDir=data['targetdir'],
                                                                                             dbname=data['targethive']
                                                                                             )

    subprocess.run(command, shell=True)
    print(command)

except Exception as e:
    print(e)

print("Successfuly table created in hive !!!!!!!!!")
