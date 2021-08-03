from subprocess import run,PIPE
import re


def countCheck(jsonData):
    dbLoc=jsonData['hiveTblName'].split(".")
    db = dbLoc[0]
    tname = dbLoc[1].lower()

    try:
        print("Checking for all records are successfully imported.................")
        command='''sqoop eval \
        --connect {connectionstr} \
        --username {id} \
        --password {passwrd} \
        --query "select count(*) from {tname}"'''.format(
            connectionstr=jsonData["connURL"],
            id=jsonData["userName"],
            passwrd=jsonData["passWord"],
            tname=jsonData["mysqlTblName"]
        ) 

        # Its working
        result = runCommand(command)
        outString=result.stdout
        if result.returncode == 0:

            # Checking with mysql table records
            mysqlCount = getCount(outString)
            print('Mysql table count is:',mysqlCount)

            # Checking with hive table records
            cmd2 = 'hdfs dfs -cat /user/hive/warehouse/'+db+'.db/'+tname+'/part-m-* | wc -l'  
            result2 = runCommand(cmd2)
            outString2=result2.stdout
            hiveCount= getCount(outString2)
            print('Hive table count is:',hiveCount)

            if mysqlCount == hiveCount :
                print('Import part is successfully complted..............')
            else:
                missingCount = mysqlCount - hiveCount
                print(missingCount,' Records are missing')

    except Exception as error:
        print(error)
		
		
		
def runCommand(cmd):
    result = run(cmd, stdout=PIPE, stderr=PIPE, universal_newlines=True,shell=True)
    return result

def getCount(outString):
    print('Checking Count...........')
    count = re.findall(r'\d+', outString)[0]
    count = int(count)
    return count