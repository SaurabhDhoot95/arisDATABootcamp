import subprocess


try:
    command = '''sqoop import \
    --connect jdbc:mysql://localhost:3306/demo \
    --username root \
    --password password \
    --table Employee \
    --incremental append \
    --check-column EmployeeID \
    --last-value 8 \
    --target-dir emp1 \
    --hive-import \
    --create-hive-table \
    --hive-table default.emp1'''

    subprocess.run(command)

except Exception as error:
    print(error)
