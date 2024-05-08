@echo off

cd /d "C:\Users\bette\Documents\Github\tesla_fsd_investigation"

C:\Users\bette\AppData\Local\Programs\Python\Python312\python.exe C:\Users\bette\Documents\Github\tesla_fsd_investigation\data_download.py

echo %DATE% %TIME Download Script complete >> C:\Users\bette\OneDrive\Desktop\nhtsa.log

pause