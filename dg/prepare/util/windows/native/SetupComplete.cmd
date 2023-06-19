goto :main

:disable_autoupdate
  reg add ^
    "HKLM\SOFTWARE\Microsoft\Windows\CurrentVersion\WindowsUpdate\Auto Update" ^
    /v AUOptions /t REG_DWORD /d 1 /f
exit /b

:setup_profiles
  rem the next line is to ease preprocessing, do not remove
  rem set profiles=
  if "%profiles%" == "" exit /b

  for %%f in (%profiles%) do set profiles_dir=%%~dpf
  if not exist "%profiles_dir%" exit /b

  if exist "%profiles%" reg import "%profiles%"

  set scripts=%~dp0
  schtasks /ru "" /create /f /tn "profiles" /sc onlogon /tr ^
    "%scripts%hidden.vbs %scripts%export.cmd %profiles%"
exit /b

:setup_drivers
  set setup_cmd=C:\drivers\setup.cmd
  if exist %setup_cmd% call %setup_cmd%
exit /b

:finish_setup
  net user Administrator /active:yes
exit /b

:main
  call :disable_autoupdate
  call :setup_drivers
  call :setup_profiles
  call :finish_setup
exit /b
